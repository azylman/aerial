# Implementation Plan: Purge Legacy SQLite Fallbacks & Unused Configs

## 1. Overview & Objective
Purge legacy SQLite configuration parameters (`db_path` in YAML, `DB_PATH` in environment variables) from `brain` and `scheduler-mcp`, and decouple the `modernc.org/sqlite` driver from production code in `scheduler-mcp` so that production strictly requires PostgreSQL 16 (`aerial-postgres`) per Invariant 7, while preserving hermetic test fixtures for in-memory unit tests.

## 2. Proposed Changes

### 2.1 Brain Configuration (`brain/pkg/config/config.go`)
- Remove `DBPath` field from `rawConfigHelper` struct.
- Remove `raw.DBPath` fallback when `raw.DatabaseURL` is empty.
- Remove `DB_PATH` lookup from `buildPostgresDSN(lookup)`.
- Update `brain/pkg/config/config_test.go` to remove assertions asserting `DB_PATH` lookup and verify PostgreSQL env vars take precedence.

### 2.2 Scheduler MCP Server (`scheduler-mcp/config.go` & `scheduler-mcp/db.go`)
- In `scheduler-mcp/config.go`:
  - Remove `DB_PATH` check in `LoadConfigFromLookup`.
  - Update error message to specify `DATABASE_URL` or `POSTGRES_HOST`.
- In `scheduler-mcp/db.go`:
  - Remove `_ "modernc.org/sqlite"` from production imports.
  - Introduce `sqliteTestInitHook func(string) (*sql.DB, error)` and `RegisterSQLiteTestHook(fn)`.
  - In `initDB(dsn string)`:
    - If `sqliteTestInitHook != nil && (isMemoryOrSqliteScheme)`, delegate to hook.
    - Strictly validate that `dsn` starts with `postgres://` or `postgresql://`. Return error otherwise.
    - Remove embedded SQLite table creation, PRAGMAs, and SQLite migration code from production binary.
  - Wire `InitDB(cfg)` to call `NewDB(cfg)`.
- In `scheduler-mcp/sqlite_test_fixture_test.go`:
  - New test file linking `_ "modernc.org/sqlite"`.
  - Implements SQLite schema setup, PRAGMAs, and migrations for unit tests (`:memory:`).
  - Automatically registers `sqliteTestInitHook` in `init()`.
- In `scheduler-mcp/server_test.go`:
  - Update tests that tested `DB_PATH` env loading to verify rejection.
  - Add `TestDB_SchemeValidationWithoutHook` to test rejection of SQLite schemes when hook is nil.

## 3. Verification & Testing Strategy
- Unit tests: Run `go test -v ./...` in `brain` and `scheduler-mcp`.
- Statement coverage: Run `./scripts/check-coverage.sh --service scheduler-mcp` (verified 95.5% >= 95.0% floor).
- Clean-room verification: Execute `./scripts/verify.sh --staged` in the scratch directory.
- Diff audit: Devil's Advocate sign-off complete.
- Scratch PR submission: Execute `/share/aerial/scripts/aerial-pr.sh submit`.
