# Implementation Plan: Dead Code Pruning Step 3 (scheduler-mcp)

## 1. Problem Statement & Scope
During the dead code static analysis and pruning initiative across the monorepo, Steps 1 (brain, PR #353) and 2 (hangar, PR #354) were completed and deployed. Step 3 targets `scheduler-mcp` to prune legacy, uncalled JSON-RPC shim handlers and unused constructor functions left behind after the migration to typed Model Context Protocol (MCP) Go SDK handlers (`mcp.AddTool`):
- `HandleScheduleRecurring(json.RawMessage)`
- `HandleScheduleOnce(json.RawMessage)`
- `HandleListSchedules(json.RawMessage)`
- `HandleCancelSchedule(json.RawMessage)`
- `HandleUpdateCronSchedule(json.RawMessage)`
- `NewConfig(databaseURL, timezone, port string)`

These methods are never called by production code (`server.go` or `main.go`). All MCP tool calls are dispatched through typed SDK methods:
- `ScheduleRecurring(ctx context.Context, args ScheduleRecurringArgs) (ScheduleRecurringOutput, error)`
- `ScheduleOnce(ctx context.Context, args ScheduleOnceArgs) (ScheduleOnceOutput, error)`
- `ListSchedules(ctx context.Context, args ListSchedulesArgs) (ListSchedulesOutput, error)`
- `CancelSchedule(ctx context.Context, args CancelScheduleArgs) (CancelScheduleOutput, error)`
- `UpdateCronSchedule(ctx context.Context, args UpdateCronScheduleArgs) (UpdateCronScheduleOutput, error)`

The only callers of the legacy `Handle*` shims are existing unit tests in `tools_test.go` and `coverage_enhancement_test.go`.

## 2. Proposed Changes (Incorporating Review Panel Feedback)

### 2.1 `scheduler-mcp/tools.go`
- Delete `HandleScheduleRecurring` (lines 210-228)
- Delete `HandleScheduleOnce` (lines 290-305)
- Delete `HandleListSchedules` (lines 341-356)
- Delete `HandleCancelSchedule` (lines 390-404)
- Delete `HandleUpdateCronSchedule` (lines 464-485)
- Remove `"encoding/json"` from `tools.go` imports to prevent compiler `imported and not used: "encoding/json"` error.
- Prune ~88 LOC of dead JSON-RPC untyped shims.

### 2.2 `scheduler-mcp/config.go`
- Delete `NewConfig(databaseURL, timezone, port string)` (lines 23-36). Production loads configuration via `LoadConfig()` or `LoadConfigFromLookup(lookup)`.
- Prune ~14 LOC of unused configuration constructor.

### 2.3 `scheduler-mcp/tools_test.go`
- Migrate `TestToolHandler_CRUD`: call typed methods `ScheduleRecurring`, `ScheduleOnce`, `ListSchedules`, `CancelSchedule` directly with typed argument structs. Verify both `Effort: "low"` and `Effort: "high"` to preserve 100% effort normalization branch coverage.
- Migrate `TestToolHandler_DefaultTimezoneFallback`: call typed `ScheduleRecurring`.
- Migrate `TestToolHandlerValidationErrors`: test validation directly against typed methods (`ScheduleRecurring`, `ScheduleOnce`, `CancelSchedule`).
- Migrate `TestHandleScheduleOnce_WithTimezone` -> `TestToolHandler_ScheduleOnce_WithTimezone`: call typed `ScheduleOnce`.
- Migrate `TestToolHandlerErrorCases`: remove raw `{bad json` checks (since parameter unmarshaling is handled by MCP SDK, verified in `server_test.go`); update domain error cases (missing required fields, unparseable date/cron, closed DB) to invoke typed methods with struct arguments.
- Prune unused `"encoding/json"` from import block if no longer required.

### 2.4 `scheduler-mcp/coverage_enhancement_test.go`
- Update `TestConfig_NewAndLoadCoverage`: remove calls to `NewConfig`. Test `LoadConfigFromLookup` and `LoadConfig` directly.
- Migrate `TestTools_HandleUpdateCronSchedule_AllBranches`: remove raw `{bad` check; update to invoke `h.UpdateCronSchedule(ctx, args)` directly with structured arguments across valid updates, effort permutations, and closed DB / nonexistent schedule branches.
- Migrate `TestTools_HandleListSchedules_EmptyAndError`: update to invoke `h.ListSchedules(ctx, args)` directly.
- Remove `TestTools_HandleListSchedules_InvalidJSON` since JSON unmarshaling is now entirely handled by the MCP SDK's `mcp.AddTool` deserializer.
- Clean up unused imports.

## 3. Verification & Gating
- Unit tests: `go test -v ./...` in `scheduler-mcp` must pass 100%.
- Coverage check: Statement coverage in `scheduler-mcp` must remain `>= 95.0%`.
- Monorepo staged verification: `./scripts/verify.sh --staged` must exit 0.
