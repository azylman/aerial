# Purge Dead Engine Configs & Deprecated Aliases Implementation Plan

**Goal:** Remove obsolete `git_sync` configuration from `brain`, retire the deprecated `classifier_model` alias and all references across `brain` in favor of `low_effort_model`, and purge legacy `/sse` to `/mcp` endpoint rewrite shims in `brain/pkg/env/mcp.go`.

**Architecture:** 
- In `brain/pkg/config`: Remove `GitSyncConfig` struct and all `GitSync` fields from `ConfigData`, `rawConfigHelper`, `UnmarshalYAML`, `cloneConfigData`, and `DefaultConfigData`. Remove `ClassifierModel` struct fields, fallback assignments, warnings, `applyEnvironmentOverrides` (line 557), and `validateChannels` (line 778), enforcing `low_effort_model` as the sole source of truth. Retain environment variable fallbacks (`LOW_EFFORT_MODEL`, `AMBIENT_CLASSIFIER_MODEL`, `CLASSIFIER_MODEL`) mapped cleanly to `data.LowEffortModel`.
- In `brain/pkg/classifier`: Update `classifier.go` and `classifier_test.go` to reference `cur.LowEffortModel` instead of `cur.ClassifierModel`.
- In `brain/pkg/queue`: Update `worker.go` to reference `cur.LowEffortModel` instead of `cur.ClassifierModel`.
- In `brain/pkg/runner`: Update `utility_daemon.go` and `utility_daemon_test.go` to reference `cur.LowEffortModel` instead of `cur.ClassifierModel`.
- In `brain/pkg/env`: Remove the `/sse` -> `/mcp` endpoint normalization loop in `LoadMCPConfig` since MCP servers natively provide Streamable HTTP endpoints. Update `env_test.go` comment and test case 5 to assert custom URLs are preserved as declared.
- Update unit tests in `brain/pkg/config` (`TestLoadConfigValidYAML`, `TestLoadConfigInterpolation`, `TestConfig_ExhaustiveDeepClone_OCP`, `TestApplyEnvironmentOverrides`, `TestConfig_SwallowedErrorsRemediationCoverage`) and verify unmarshaling behavior on unknown YAML keys.
- Clean up references in `config.example.yaml`, `README.md`, and `GEMINI.md`.

**Tech Stack:** Go (1.24+), YAML v3, JSON

## Global Constraints
- Zero downtime, zero breaking changes to existing valid configs.
- Unrecognized YAML keys in user `config.yaml` must not fail unmarshaling.
- All unit tests must pass with race detection enabled (`go test -race`).
- Fast pre-flight verification (`./scripts/verify.sh --staged`) must pass cleanly.

---

### Task 1: Clean Up `brain/pkg/config/config.go` and `config_test.go`

**Files:**
- Modify: `brain/pkg/config/config.go`
- Modify: `brain/pkg/config/config_test.go`

**Changes in `config.go`:**
- Remove `GitSyncConfig` struct.
- Remove `GitSync` field from `ConfigData`, `rawConfigHelper`, `UnmarshalYAML`, `cloneConfigData`, `DefaultConfigData`.
- Remove `ClassifierModel` from `ConfigData`, `rawConfigHelper`, `UnmarshalYAML`, `DefaultConfigData`.
- In `applyEnvironmentOverrides`: Remove `data.ClassifierModel = lem`; keep `data.LowEffortModel = lem`.
- In `validateChannels`: Remove `parsed.ClassifierModel = DefaultConfigData().LowEffortModel`.

**Changes in `config_test.go`:**
- In `TestLoadConfigValidYAML`: Remove `GitSync` assertion.
- In `TestLoadConfigInterpolation`: Change interpolation target from `git_sync.config_repo_url` to `system_channel` or another active field.
- In `TestConfig_ExhaustiveDeepClone_OCP`: Remove `GitSync` deep copy assertions.
- In `TestApplyEnvironmentOverrides`: Update assertions to inspect `d.LowEffortModel` instead of `d.ClassifierModel`.
- In `TestConfig_SwallowedErrorsRemediationCoverage`: Update subtest 1 to verify unknown YAML fields (like `git_sync`) unmarshal safely without error.

---

### Task 2: Update Callers of `ClassifierModel` Across `classifier`, `queue`, and `runner`

**Files:**
- Modify: `brain/pkg/classifier/classifier.go`
- Modify: `brain/pkg/classifier/classifier_test.go`
- Modify: `brain/pkg/queue/worker.go`
- Modify: `brain/pkg/runner/utility_daemon.go`
- Modify: `brain/pkg/runner/utility_daemon_test.go`

**Changes:**
- In `brain/pkg/classifier/classifier.go` (line 388): Replace `cur.ClassifierModel` fallback with `cur.LowEffortModel`.
- In `brain/pkg/classifier/classifier_test.go` (line 751): Change `ClassifierModel: "custom-flash-model"` to `LowEffortModel: "custom-flash-model"`.
- In `brain/pkg/queue/worker.go` (line 1147): Replace `flashModel = cur.ClassifierModel` with `flashModel = cur.LowEffortModel`.
- In `brain/pkg/runner/utility_daemon.go` (line 125): Replace `cur.ClassifierModel` fallback with `cur.LowEffortModel`.
- In `brain/pkg/runner/utility_daemon_test.go` (lines 277-294): Update test cases to test `LowEffortModel` instead of deprecated `ClassifierModel`.

---

### Task 3: Purge Legacy `/sse` Endpoint Rewrite in `brain/pkg/env/mcp.go` and `env_test.go`

**Files:**
- Modify: `brain/pkg/env/mcp.go`
- Modify: `brain/pkg/env/env_test.go`

**Changes:**
- In `brain/pkg/env/mcp.go`: Remove lines 116–126 (`// 4. Normalize legacy SSE endpoints to Streamable HTTP`).
- In `brain/pkg/env/env_test.go`:
  - Fix comment at line 195.
  - In subtest 5 of `TestLoadMCPConfig_Coverage` (or `TestLoadMCPConfig_FileOverridesAndNormalizations`), assert that custom endpoints ending in `/sse` or `/mcp` are preserved as declared.

---

### Task 4: Update System Documentation and Example Configuration

**Files:**
- Modify: `config.example.yaml`
- Modify: `README.md`
- Modify: `GEMINI.md`

**Changes:**
- Remove `git_sync` stanza from `config.example.yaml`.
- Remove `git_sync` stanza from `README.md`.
- Remove `git_sync` from `GEMINI.md` user options listing.
