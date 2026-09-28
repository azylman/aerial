# Target-Isolated Skills, MCP Configuration & Dual Process Pools Design Specification

## 1. Overview & Architectural Goals

This specification extends Aerial's target isolation architecture (introduced for rules in PR #399) across the entire runtime environment:
- Explicit target categorization for all skills (`common`, `discord`, `voice`). Root/flat un-nested skills are strictly disallowed and ignored with zero legacy fallback.
- Explicit target categorization for custom MCP servers in `config.yaml` (`common`, `discord`, `voice`). Root/flat un-nested MCP server definitions are strictly disallowed with zero legacy fallback.
- Partitioning of built-in MCP defaults so that voice runtimes are never burdened with developer or Discord tools (`discord`, `docker`, `github`, `openobserve`, `victoriametrics`).
- Dual application-wide singleton process pools (`UnifiedProcessPool`) for Discord vs. Voice, where the target `HOME` runtime directory is a native property of the pool itself.
- Superpowers skills (`/opt/skills`) and core development skills (`self-improvement`, `incident-triage`) are strictly bound to `discord` only and never exposed to `voice`.
- Clean excision of MCP config copying and skills symlinking from `provisionRuntimeSharedAssets` in `rules.go`, ensuring runtime boundaries are governed strictly by dedicated provisioner subsystems.

---

## 2. Directory Hierarchy & Skill Categorization

### 2.1 File System Organization

All skills must reside within an explicit target subdirectory:

- **Core Skills (`.agents/skills/`)**:
  - `.agents/skills/common/<skill-name>/`
  - `.agents/skills/discord/<skill-name>/`
  - `.agents/skills/voice/<skill-name>/`
  - Relocations:
    - Move `.agents/skills/self-improvement` to `.agents/skills/discord/self-improvement`.
    - Move `.agents/skills/incident-triage` to `.agents/skills/discord/incident-triage`.
  - Invariant: Any skill located directly in `.agents/skills/` without a target parent folder is ignored with a logged structured warning.

- **User Custom Skills (`aerial-config/custom-skills/`)**:
  - `custom-skills/common/<skill-name>/`
  - `custom-skills/discord/<skill-name>/`
  - `custom-skills/voice/<skill-name>/`
  - Invariant: Any skill located directly in `custom-skills/` without a target parent folder is ignored with a logged structured warning.

- **Superpowers (`/opt/skills` or `/opt/superpowers/skills`)**:
  - Exclusively provisioned into the `discord` runtime.
  - Never provisioned or linked into `voice`.

### 2.2 Provisioning & Reconciliation Logic (`brain/pkg/env/skills.go`)

- **Torn-Read & Validity Checking**:
  - For any candidate skill directory, verify `SKILL.md` exists and satisfies `fi.Size() > 0` before treating as a valid skill. Empty files resulting from torn reads during git sync are skipped.
- **Missing Directory Semantics**:
  - If a source directory does not contain `voice/`, `discord/`, or `common/`, `os.IsNotExist` is treated cleanly as an empty set without error.
- **Reconciliation & Stale Symlink Pruning (`reconcileTargetSkills`)**:
  - When linking skills into `targetSkillDir` (e.g. `runtimes/voice/.gemini/config/skills`):
    - Scan allowed skills across `{source}/common/` and `{source}/{target}/` (plus `superpowers` if `target == TargetDiscord`).
    - Build `allowedSkills map[string]bool`.
    - Inspect existing entries in `targetSkillDir`: any symlink or file whose name is NOT in `allowedSkills` is immediately removed. This ensures skills moved from `common` to `discord` are pruned from `voice` runtimes on subsequent syncs.
    - Atomically create/update symlinks for all allowed skills.
    - Run final orphan sweep (`sweepOrphanedSymlinks`).
- **Provisioning Execution in `SyncSkills()`**:
  - Provisions `runtimes/discord/.gemini/config/skills/` with `common` + `discord` + `superpowers`.
  - Provisions `runtimes/voice/.gemini/config/skills/` with `common` + `voice` only.
  - Provisions primary `~/.gemini/config/skills/` with `common` + `discord` + `superpowers` for CLI compatibility.
- **Excision from `rules.go`**:
  - Remove all skills symlinking (`primarySkills -> targetSkills` and `legacySkills -> targetLegacySkills`) from `provisionRuntimeSharedAssets` in `brain/pkg/env/rules.go`.

---

## 3. MCP Target Partitioning & Schema Specification

### 3.1 Configuration Schema (`brain/pkg/config/config.go`)

Update `ConfigData.McpServers` in `brain/pkg/config/config.go` to an explicit target-categorized struct:

```go
type TargetMcpConfig struct {
    Common  map[string]json.RawMessage `yaml:"common,omitempty" json:"common,omitempty"`
    Discord map[string]json.RawMessage `yaml:"discord,omitempty" json:"discord,omitempty"`
    Voice   map[string]json.RawMessage `yaml:"voice,omitempty" json:"voice,omitempty"`
}
```

- **Deep Copy in `cloneConfigData`**:
  - Explicitly allocate new maps and clone every entry for `Common`, `Discord`, and `Voice` to maintain immutability across `cfg.Current()` snapshots.
- **YAML Unmarshaling & Zero-Legacy Validation**:
  - Update `rawConfigHelper` to decode `mcp_servers` as `TargetMcpConfig`.
  - Root-level un-nested server keys in `mcp_servers` are rejected or unmapped, enforcing explicit target categorization.
- **Default Config**:
  - Initialize empty maps in `DefaultConfigData`.

### 3.2 Built-In MCP Defaults Partitioning (`brain/pkg/env/mcp.go`)

- **Discord Built-ins**:
  - `scheduler`: `http://scheduler-mcp:8080/mcp`
  - `discord`: `http://discord-mcp:4001/mcp`
  - `docker`: `http://docker-mcp:4002/mcp`
  - `victoriametrics`: `http://victoriametrics-mcp:4004/mcp`
  - `github`: `http://github-mcp:4003/mcp` (if `GitHubPAT` is configured)
  - `openobserve`: `http://openobserve:5080/openobserve/api/...` (if credentials are configured)
- **Voice Built-ins**:
  - `scheduler`: `http://scheduler-mcp:8080/mcp`
  - Zero discord, zero docker, zero github, zero victoriametrics, zero openobserve.

### 3.3 Merging & Target File Emission

- **Target Config Compilation**:
  - `LoadTargetMCPConfig(cfg *config.Config, target RuleTarget) json.RawMessage`:
    - For `TargetDiscord`: Built-ins (Discord) + `mcp_servers.common` + `mcp_servers.discord`.
    - For `TargetVoice`: Built-ins (Voice) + `mcp_servers.common` + `mcp_servers.voice`.
- **Atomic File Writing**:
  - Refactor `EnsureMcpConfig` into `EnsureTargetMcpConfig(targetGeminiDir string, rawConfig json.RawMessage) error` using `writeAtomic`.
  - Atomically write:
    - `<dataDir>/runtimes/discord/.gemini/config/mcp_config.json`
    - `<dataDir>/runtimes/voice/.gemini/config/mcp_config.json`
    - Primary `<homeDir>/.gemini/config/mcp_config.json` (mirrors Discord).
- **Excision from `rules.go`**:
  - Remove all MCP config copying (`primaryMcp -> targetMcp`) from `provisionRuntimeSharedAssets` in `brain/pkg/env/rules.go`.

---

## 4. Dual Singleton Process Pools

### 4.1 Pool Configuration & Environment Injection (`brain/pkg/runner/`)

- **`PoolConfig` Definition (`brain/pkg/runner/unified_pool.go`)**:
  ```go
  type PoolConfig struct {
      PrewarmedTargets []string
      DefaultModel     string
      TargetModels     map[string]string
      AgyBin           string
      Cwd              string
      GeminiHomeDir    string
      MaxIdle          time.Duration
      Env              []string
  }
  ```
- **Environment Sanitization in `BuildAgyEnv` (`brain/pkg/runner/pure.go`)**:
  - Proactively strip any existing `HOME=`, `USERPROFILE=`, and `GEMINI_CLI_HOME=` from `BaseEnv`.
  - When `input.HomeDir` is provided, append:
    - `HOME=` + `input.HomeDir`
    - `USERPROFILE=` + `input.HomeDir`
    - `GEMINI_CLI_HOME=` + `input.HomeDir`
  - This eliminates duplicate environment keys on Linux where libc `getenv` returns the first occurrence.
- **Daemon Environment Construction in `UnifiedProcessPool.GetOrCreate`**:
  - Ensure `p.cfg.GeminiHomeDir` and its `.gemini` directory exist on disk before spawning (created with `0755` permissions if missing).
  - Construct `daemonCfg.Env` by running `BuildAgyEnv(AgyEnvInput{ BaseEnv: p.cfg.Env, HomeDir: p.cfg.GeminiHomeDir, ... })`.
  - Pass `daemonCfg` to `p.spawner.Spawn`.

### 4.2 Application Singletons in `brain/main.go`

Construct two distinct singletons during application boot:

- **Discord Process Pool**:
  - `GeminiHomeDir: filepath.Join(dataDir, "runtimes", "discord")`
  - Pre-warmed targets: `ephemeral:classifier`, `ephemeral:summarizer`
  - Target models: low-effort for ephemeral targets, default for Discord threads
  - Injected into `WorkerPoolConfig.ProcessPool`
  - Handles all incoming Discord turns, ambient message classification, and thread title summarization.

- **Voice Process Pool**:
  - `GeminiHomeDir: filepath.Join(dataDir, "runtimes", "voice")`
  - Pre-warmed targets: `kiosk`
  - Target models: low-effort for `kiosk`
  - Injected into `WorkerPoolConfig.VoiceProcessPool`
  - Handles all incoming requests to `/api/voice/ask` and `/voice/ask` via `WorkerPool.ExecuteVoiceTurn`.

- **Startup Pre-warming**:
  - In `brain/main.go`, invoke `discordPool.Initialize(initCtx)` and `voicePool.Initialize(initCtx)` with a detached background context (`5 * time.Second` timeout) so pre-warmed targets are eagerly launched.

- **Voice Turn Target Routing Policy (Strict Payload Device Identifier)**:
  - The caller must explicitly supply `session_id` (or `conversation_id`) in the JSON request payload to identify the physical device (e.g. `"kiosk"`).
  - If `session_id` is empty or missing, `handleVoiceAsk` returns HTTP 400 Bad Request (`"Invalid payload: 'session_id' (or 'conversation_id') is required to identify the device"`).
  - In `WorkerPool.ExecuteVoiceTurn`, the target key in `VoiceProcessPool` is the sanitized device/session identifier directly (e.g. `"kiosk"` routes to the pre-warmed `"kiosk"` daemon). If empty, it returns an error immediately. Zero random UUID generation, zero header/IP inspection.

### 4.3 Lifecycle & Teardown Symmetry

- In `WorkerPool.Stop()`:
  - Close `p.processPool`.
  - Close `p.voiceProcessPool`.
- In `brain/main.go`:
  - `defer discordPool.Close()` and `defer voicePool.Close()` execute after `pool.StopWithTimeout(10 * time.Second)` and HTTP server shutdown, ensuring active turns cleanly finish before daemons are killed.

---

## 5. Non-Functional Invariants & Test Coverage Plan

- Strictly ZERO markdown tables across all artifacts, messages, commits, and summaries (bulleted lists only).
- Strictly ZERO swallowed errors. All errors are logged with structured context or propagated.
- Statement coverage floor of strictly >= 95.0% maintained across all modified packages:
  - `brain/pkg/env`: Test target-based skills discovery, torn-read size validation, stale symlink reconciliation, target MCP compilation, and excision from `provisionRuntimeSharedAssets`.
  - `brain/pkg/config`: Test `TargetMcpConfig` unmarshaling, deep cloning in `cloneConfigData`, and zero-legacy flat key rejection.
  - `brain/pkg/runner`: Test `BuildAgyEnv` environment deduplication/stripping, `GeminiHomeDir` creation, and dual pool idle reaping.
  - `brain/pkg/queue`: Test dual pool injection, voice turn routing to `kiosk` vs `voice-<uuid>`, and symmetrical teardown in `WorkerPool.Stop()`.
  - `brain`: Test dual pool construction, pre-warming initialization, and shutdown sequence.
- All tests hermetic: in-memory `io.Pipe()`, `t.TempDir()`, zero OS subprocess execution during testing.
- Pre-commit fast verification (`scripts/verify.ps1 -Staged`) passes cleanly with zero lint or deadcode warnings.
