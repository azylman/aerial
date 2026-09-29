# Implementation Plan - TargetEphemeral Runtime Isolation

## 1. Overview & Architectural Objective
Introduce `TargetEphemeral` runtime profile (`runtimes/ephemeral`) to decouple fast-path LLM operations (ambient message classification, thread titling, session rotation, action summarization) from the heavy Discord runtime. This strips persona, death metal, emojis, UI/visual rules, voice constraints, MCP schemas, and skill definitions (~20k tokens per prompt) while retaining core technical domain rules (Aerial identity, invariants, user identity) and enforcing strict JSON/XML/plain-text output constraints.

---

## 2. Review Recommendations Incorporated (The Girl Gang Feedback)
1. **Strict Common Rule Whitelisting**: For `10_user_common_` rules under `TargetEphemeral`, explicitly whitelist `01-identity.md` (only identity and core facts), strictly omitting all persona, communication style, or behavior rules rather than relying on brittle substring filtering.
2. **Precedence-Enforcing Output Constraints**: Author the ephemeral constraints rule as `rules/ephemeral/99-output-constraints.md` so its lexical sorting ensures it loads last, superseding any latent conversational traits.
3. **Graceful Directory Missing Semantics**: Ensure `discoverRuleFiles` cleanly handles non-existent `rules/ephemeral/` directories in `aerial-config` (`os.IsNotExist` returns `nil, nil`).
4. **Defensive Nil Safety**: In `cloneConfigData` and `rawConfigHelper`, defensively guard against `nil` or `null` `mcp_servers.ephemeral` entries.
5. **Execution Order & Invariant Protection**: In `brain/main.go`, ensure `envProvisioner.Sync()` fully completes before `discordLowEffortPool` is constructed. Guard `DataDir` resolution when constructing `ephemeralHome`.

---

## 3. Tasks & Implementation Steps (~15-Minute Granularity)

### Task 1: Core Target Rules & Rule Profile (`brain/pkg/env/rules.go` & `rules/ephemeral/`)
- Add `TargetEphemeral RuleTarget = "ephemeral"` to `brain/pkg/env/rules.go`.
- In `compileTargetRules(target RuleTarget, customPrompt string)`:
  - If `target == TargetEphemeral`:
    - For `10_user_common_`, only allow files matching the whitelist (specifically `01-identity.md`).
    - Discover `rules/ephemeral/` from aerial and aerial-config roots (`20_aerial_ephemeral_`, `30_user_ephemeral_`).
  - In `SyncRules`:
    - Compile `ephemeralRules` via `p.compileTargetRules(TargetEphemeral, "")`.
    - When `p.dataDir != ""`, sync to `filepath.Join(p.dataDir, "runtimes", "ephemeral", ".gemini")` and call `p.provisionRuntimeSharedAssets(ephemeralHome)`.
- Create `rules/ephemeral/99-output-constraints.md` in `azylman/aerial`:
  - Enforce raw JSON/XML or single-line plain text output, zero markdown wrappers unless explicitly requested, zero conversational filler/pleasantries.

### Task 2: Target MCP Configuration & Schema (`brain/pkg/config/` & `brain/pkg/env/mcp.go`)
- In `brain/pkg/config/config.go`:
  - Add `Ephemeral map[string]json.RawMessage` to `TargetMcpConfig`.
  - Update `cloneConfigData` to safely deep copy `Ephemeral` (checking for `src.McpServers.Ephemeral != nil`).
  - Update unmarshaler to accept `ephemeral` in `mcp_servers` mapping (`common`, `discord`, `voice`, `ephemeral`).
  - Update `DefaultConfigData` to initialize `Ephemeral: make(map[string]json.RawMessage)`.
- In `brain/pkg/env/mcp.go`:
  - In `LoadTargetMCPConfig(cfg *config.Config, target RuleTarget)`:
    - Only add default `scheduler` MCP if `target != TargetEphemeral`.
    - If `target == TargetEphemeral`, only include `cur.McpServers.Ephemeral` (do not inherit `cur.McpServers.Common`).
  - In `SyncMCP`:
    - Compile `ephemeralConfig := p.LoadTargetMCPConfig(cfg, TargetEphemeral)`.
    - Sync to `runtimes/ephemeral/.gemini/config/mcp_config.json` via `EnsureTargetMcpConfig`.

### Task 3: Target Skills Isolation (`brain/pkg/env/skills.go`)
- In `brain/pkg/env/skills.go`:
  - In `LinkTargetSkills(targetDir string, target RuleTarget)`:
    - If `target == TargetEphemeral`, scan only `[]string{string(target)}` (only `ephemeral`, strictly excluding `common`, `discord`, and `voice`).
    - Superpowers are excluded.
  - In `SyncSkills`:
    - Link skills into `runtimes/ephemeral/.gemini/config/skills` via `LinkTargetSkills(ephemeralRuntimeDir, TargetEphemeral)`.

### Task 4: Session Storage Discovery & Process Pool Wiring (`brain/pkg/session/` & `brain/main.go`)
- In `brain/pkg/session/session.go`:
  - Include `"ephemeral"` in runtime lists for `getTargetDirs` and `Roots` (`"discord"`, `"voice"`, `"ephemeral"`).
- In `brain/main.go`:
  - Define `ephemeralHome := filepath.Join(cur.DataDir, "runtimes", "ephemeral")` (fallback to `p.homeDir` if `cur.DataDir` empty).
  - Ensure `envProvisioner.Sync()` has executed.
  - Wire `discordLowEffortPool` (`runner.PoolConfig.GeminiHomeDir`) to `ephemeralHome`.

### Task 5: Hermetic Unit Tests & Coverage Validation
- `brain/pkg/config/config_test.go`:
  - Test `mcp_servers.ephemeral` unmarshaling, rejection of invalid categories, null category handling, and deep copy isolation.
- `brain/pkg/env/env_test.go`:
  - Test `compileTargetRules` for `TargetEphemeral`: confirm persona exclusion, inclusion of `99-output-constraints.md`, and retention of common identity/invariants.
  - Test `LoadTargetMCPConfig` for `TargetEphemeral`: verify 0 MCP servers by default (no scheduler, no common tools).
  - Test `LinkTargetSkills` for `TargetEphemeral`: verify 0 skills linked by default.
  - Test `SyncRules`, `SyncMCP`, `SyncSkills` creating isolated files in `runtimes/ephemeral/.gemini`.
- `brain/pkg/session/session_test.go`:
  - Test session and transcript discovery under `runtimes/ephemeral`.
- `brain/main_test.go`:
  - Verify `discordLowEffortPool` receives `ephemeralHome`.
