# Design Specification: Hermetic Configuration & Pure Constructor Dependency Injection

**Date**: 2026-09-06  
**Status**: Revised (Post-Audit Hardened)  
**Target Repositories**: `azylman/aerial`  
**Scope**: `brain/pkg/config`, `brain/pkg/db`, `brain/pkg/memory`, `brain/pkg/gitsync`, `brain/pkg/sanitizer`, `brain/pkg/scheduler`, `brain/pkg/queue`, `brain/main.go`, `scheduler-mcp`, `scripts/verify.sh`, `scripts/check-coverage.sh`, `scripts/migrate_sqlite_to_postgres.go`, `.github/workflows/docker-publish.yml`.

---

## 1. Executive Summary & Problem Statement

### 1.1 The Incident & Root Cause
During automated PR submission (`aerial-pr.sh submit`), a false-alarm "poison pill" alert was dispatched to Discord for an in-flight message. Thorough post-incident diagnosis revealed that the production runtime never crashed. Instead:
1. `go test` executed inside the production container during pre-commit validation.
2. The test runner script (`scripts/verify.sh`) attempted to isolate tests by unsetting `DATABASE_URL` via `env -u DATABASE_URL`.
3. However, `POSTGRES_HOST=postgres` and associated `POSTGRES_*` environment variables remained set in the container environment.
4. When unit tests invoked database initialization or test fixtures, `pkg/db.GetDBPath()` inspected the ambient environment, fell back to `POSTGRES_*`, and connected directly to the **live production PostgreSQL database**.
5. A test queue processor found the production in-flight message in `StatusProcessing`, incremented its retry count to 3, and dispatched a poison-pill alert to Discord.

### 1.2 The Architectural Flaw: Ambient Environment Coupling
The root failure was not a missing flag in a test script; it was architectural:
- **Ambient Environment Leakage**: Sub-packages (`pkg/db`, `pkg/memory`, `pkg/gitsync`, `pkg/sanitizer`, `scheduler-mcp`, `pkg/scheduler`) directly called `os.Getenv` or `config.GetEnv` to resolve defaults instead of receiving explicit dependencies from their callers.
- **Fragile Denylists**: Attempting to unset individual environment variables (`env -u DATABASE_URL`) fails whenever new infrastructure variables are introduced.
- **Test Harness Contamination**: Tests probed `TEST_DATABASE_URL` and ambient fallback paths instead of operating hermetically with pure in-memory or temporary resources.
- **Toxic Defaults**: When `POSTGRES_HOST` was unset, code automatically generated connection strings pointing to `postgres:5432`, creating startup hangs (~27.5s) on non-Docker local environments.

### 1.3 The Solution: Pure Constructor Dependency Injection & Clean-Room Testing
We enforce five architectural invariants across the monorepo:
1. **Zero `os.Getenv` in Sub-packages**: No package in `brain/pkg/*` (except `brain/pkg/config`) and no MCP tool module may read the process environment. `config.GetEnv` is unexported (`getEnv`) to prevent backdoors.
2. **Single Parsing Authority**: `brain/pkg/config` is the sole parser of environment variables and configuration files for `brain`.
3. **Pure Constructor Dependency Injection**: Every package constructor requires its configuration and dependencies explicitly (e.g. `InitDB(dsn string)`, `NewClient(cfg memory.ClientConfig)`). If a required parameter is empty, constructors return an immediate error rather than falling back to ambient state.
4. **Zero Infrastructure Environment Variables in Tests**: Tests never read `os.Getenv` or call `t.Setenv` for database, auth, or infrastructure credentials. `TEST_DATABASE_URL` is deleted entirely. Tests pass explicit test values, `filepath.Join(t.TempDir(), "test.db")`, or `":memory:"`. (Only `brain/pkg/config/config_test.go` and `dashboard/main_test.go` retain scoped `t.Setenv` to test config file/env fallback parsing).
5. **Cross-Platform Clean-Room Harness**: Test harnesses (`scripts/verify.sh` and `scripts/check-coverage.sh`) execute `go test` under an OS-aware clean-room allowlist supporting Linux, macOS, and Windows/MSYS2 toolchains without leaking credentials.

---

## 2. Architectural Invariants & Guarantees

| Invariant | Description | Enforcement Mechanism |
|---|---|---|
| **I1: Single Parsing Authority** | Only `brain/pkg/config` reads `os.Getenv` for `brain`. Subpackages cannot call `config.GetEnv`. `main.go` calls `config.LoadConfig()` and passes typed fields. | Static analysis check (`git grep "os.Getenv" brain/pkg/ \| grep -v "brain/pkg/config"` returns 0, and `git grep "config.GetEnv" brain/pkg/` returns 0). |
| **I2: Pure Constructor DI** | Sub-package constructors require all credentials, connection strings, and endpoints as explicit parameters or typed config structs. | No fallback to `os.Getenv`. `db.InitDB("")` returns an explicit error. Unrecognized URL schemes return errors instead of falling through to SQLite. |
| **I3: Zero Test Env Usage for Infrastructure** | Tests never read `os.Getenv` or mutate env via `t.Setenv` for credentials or infrastructure. | No `TEST_DATABASE_URL`. Tests instantiate dependencies with pure test fixtures. |
| **I4: Clean-Room Harness** | `verify.sh` and `check-coverage.sh` run tests using OS-aware clean-room wrappers. Failures in coverage generation are recorded as blocking violations. | No ambient credentials can leak into the test process. |
| **I5: Complete Excision of `TEST_DATABASE_URL`** | `TEST_DATABASE_URL` is completely removed from all source code, tests, scripts, and workflows. | `git grep "TEST_DATABASE_URL"` returns 0 across the entire repository. |

---

## 3. Detailed Component Refactoring

### 3.1 `brain/pkg/config`: Single Source of Truth
`brain/pkg/config` expands to become the comprehensive configuration loader for `brain`:
- **Expanded `Config` Struct**:
  ```go
  type Config struct {
      // Existing fields
      Model         string                     `yaml:"model" json:"model"`
      Timezone      string                     `yaml:"timezone" json:"timezone"`
      SystemChannel string                     `yaml:"system_channel" json:"system_channel"`
      AdminUsers    []string                   `yaml:"admin_users" json:"admin_users"`
      Channels      map[string]ChannelPolicy   `yaml:"channels" json:"channels"`
      GitSync       GitSyncConfig              `yaml:"git_sync" json:"git_sync"`
      McpServers    map[string]json.RawMessage `yaml:"mcp_servers,omitempty" json:"mcp_servers,omitempty"`

      // Infrastructure & Runtime Configuration (parsed by pkg/config)
      DatabaseURL     string       `yaml:"database_url" json:"database_url"`
      Port            string       `yaml:"port" json:"port"`
      AgyBin          string       `yaml:"agy_bin" json:"agy_bin"`
      APIKey          string       `yaml:"api_key" json:"api_key"`
      SystemPrompt    string       `yaml:"system_prompt" json:"system_prompt"`
      DiscordToken    string       `yaml:"discord_token" json:"discord_token"`
      GitHubPAT       string       `yaml:"github_pat" json:"github_pat"`
      Ollama          OllamaConfig `yaml:"ollama" json:"ollama"`
      ClassifierModel string       `yaml:"classifier_model" json:"classifier_model"`
  }

  type OllamaConfig struct {
      BaseURL     string `yaml:"base_url" json:"base_url"`
      Model       string `yaml:"model" json:"model"`
      QueryPrefix string `yaml:"query_prefix" json:"query_prefix"`
  }
  ```
- **Environment Resolution & Safe Defaulting**:
  - `DatabaseURL`: Parsed from `DATABASE_URL`. If unset, checked for `POSTGRES_HOST`.
    - If `POSTGRES_HOST` is set, synthesize URL using `net/url` and `net.JoinHostPort`:
      `postgres://user:pass@host:port/dbname?sslmode=...`
    - If `POSTGRES_HOST` is unset, default to local SQLite: `filepath.Join(dataDir, "aerial.db")` (or empty string in pure DefaultConfig so tests/callers configure explicitly). **Never default to non-existent hostname `postgres:5432`**.
  - `Port`: `PORT` (default `"8080"`).
  - `AgyBin`: `AGY_BIN` (default `"agy"`).
  - `APIKey`: `GEMINI_API_KEY` or `ANTIGRAVITY_API_KEY`.
  - `SystemPrompt`: `SYSTEM_PROMPT`.
  - `DiscordToken`: `DISCORD_TOKEN` or `DISCORD_BOT_TOKEN`.
  - `GitHubPAT`: `GITHUB_PAT`.
  - `Ollama`: `OLLAMA_URL` (default `"http://localhost:11434"`), `EMBEDDING_MODEL` or `OLLAMA_EMBEDDING_MODEL` (default `"nomic-embed-text"`), `EMBEDDING_QUERY_PREFIX` (default `"search_query: "`).
  - `ClassifierModel`: `CLASSIFIER_MODEL` or `AMBIENT_CLASSIFIER_MODEL` (default `"Gemini 3.8 Flash (Low)"`).
- **LoadConfigFromPaths Fallback Merging**:
  When unmarshaling `config.yaml`, any empty infrastructure fields in `parsed` MUST inherit from `fallback` so omitted fields in `config.yaml` do not cause zero-value startup crashes.
- **Unexport `GetEnv`**:
  Make `getEnv(key, defaultVal string) string` internal to `brain/pkg/config`.
- **Function Refactoring**:
  - `GetTimezone()` and `GetSystemChannel()`: operate solely on `GetRuntimeConfig()`, removing direct `os.Getenv` fallbacks.
  - `EnsureAgySettings`: accepts explicit API key and home directory.
  - `EnsureMcpConfig`: accepts explicit GitHub PAT and home directory.
  - `LoadMCPConfig(pat string)`: accepts `pat` explicitly from `Config.GitHubPAT`, removing `os.Getenv("GITHUB_PAT")`.

### 3.2 `brain/pkg/db`: Strict Constructor Requirement & Driver Invariants
- **Delete `GetDBPath()`**: Removed entirely from `brain/pkg/db`.
- **Hardened `InitDB(dsn string)`**:
  ```go
  func InitDB(dsn string) (*sql.DB, error) {
      trimmed := strings.TrimSpace(dsn)
      if trimmed == "" {
          return nil, fmt.Errorf("db: connection string cannot be empty")
      }

      isPg := strings.HasPrefix(trimmed, "postgres://") || strings.HasPrefix(trimmed, "postgresql://")
      if !isPg {
          if strings.Contains(trimmed, "://") && !strings.HasPrefix(trimmed, "file://") {
              return nil, fmt.Errorf("db: unsupported database scheme in %q", trimmed)
          }
      }

      if isPg {
          // Pre-flight validation with pgx before entering retry loop
          if _, err := stdlib.ParseConfig(trimmed); err != nil {
              return nil, fmt.Errorf("db: invalid postgres connection string: %w", err)
          }
          // ... retry loop & pool setup ...
      } else {
          // SQLite: SetMaxOpenConns(1) for in-memory databases
          if trimmed == ":memory:" || strings.Contains(trimmed, "mode=memory") {
              database.SetMaxOpenConns(1)
          }
      }
      return database, nil
  }
  ```
- **Hermetic Testing in `db_test.go`**:
  - Delete `TestGetDBPath_Precedence` and `TestDBPath`.
  - In `setupTestDB()`: remove `os.Getenv("TEST_DATABASE_URL")` and fallback to live Postgres. Tests strictly create a temporary SQLite database via `filepath.Join(t.TempDir(), "test.db")` or `":memory:"`.

### 3.3 `brain/pkg/memory`: Pure Client Constructor
- **Decoupled Configuration**:
  ```go
  type ClientConfig struct {
      BaseURL     string
      Model       string
      QueryPrefix string
  }

  type Client struct {
      BaseURL     string
      Model       string
      QueryPrefix string
      HTTPClient  *http.Client
  }

  func NewClient(cfg ClientConfig) *Client
  ```
- **Delete all `os.Getenv` in `ollama.go`**: Lines 30, 64, 69, 71 removed.
- **Hermetic Testing in `memory_test.go`**:
  - Remove `os.Getenv("TEST_DATABASE_URL")`. Tests construct `NewClient(memory.ClientConfig{BaseURL: server.URL, ...})` directly.

### 3.4 `brain/pkg/gitsync`: Explicit Token Injection
- **`SyncRepo` Signature**:
  ```go
  func SyncRepo(ctx context.Context, repoPath string, pat string) (bool, error)
  ```
- **Delete `os.Getenv("GITHUB_PAT")`**: Token is passed from the caller.

### 3.5 `brain/pkg/sanitizer`: Explicit Sensitive Token Registration
- **Token Registration**:
  - Provide `RegisterSensitiveTokens(tokens ...string)` and `ResetSensitiveTokens()` (for test teardown).
  - Use real struct identifiers: `envMu.Lock()`, `envSecretsCache`, `sort.Slice`.
  - In `main.go`, invoke `sanitizer.RegisterSensitiveTokens(cfg.APIKey, cfg.DiscordToken, cfg.GitHubPAT)` on startup AND during configuration hot-reload in `CreateReloadConfigFunc`.

### 3.6 `brain/pkg/scheduler`: Pure Fact Extraction
- **Refactor `ExtractFactsLLM`**:
  Eliminate `config.GetEnv`. Refactor `ExtractFactsLLM` to accept explicit parameters or provide `NewFactExtractionLLM(agyBin, apiKey, model string, runnerFn runner.RunnerFunc) memory.LLMClientFunc`.
- Update `scheduler.Start` to receive `memClient *memory.Client` and fact extraction runner closure.

### 3.7 `scheduler-mcp`: Service-Level Isolation
- **Delete `GetDBPath()`**: Removed from `scheduler-mcp/db.go`.
- **Hardened `InitDB(dsn string)`**: Strictly requires non-empty `dsn`. Sets `SetMaxOpenConns(1)` for SQLite `:memory:`. Uses `SELECT pg_advisory_lock(849201948201)` during PostgreSQL schema migrations.
- **Explicit Config in `main.go`**:
  - `main.go` reads `PORT`, `DATABASE_URL` / `POSTGRES_*`, `DEFAULT_TIMEZONE` / `TZ`.
  - Injects `timezone` into `ToolHandler` struct (`NewToolHandler(database, timezone)`) rather than a package global variable.
- **Hermetic Testing**:
  - Delete `TestPostgresSchedules` from `tools_test.go`. Remove all `TEST_DATABASE_URL` checks and `GetDBPath` tests. All tests use SQLite temp files or `":memory:"`.

### 3.8 `brain/main.go`: Pure Composition Root
- `main.go` acts solely as the composition root:
  1. `cfg, err := config.LoadConfig()`
  2. `database, err := db.InitDB(cfg.DatabaseURL)`
  3. `sanitizer.RegisterSensitiveTokens(cfg.APIKey, cfg.DiscordToken, cfg.GitHubPAT)`
  4. `memClient := memory.NewClient(memory.ClientConfig{BaseURL: cfg.Ollama.BaseURL, Model: cfg.Ollama.Model, QueryPrefix: cfg.Ollama.QueryPrefix})`
  5. `bCfg := NewBrainConfig(cfg)`
  6. Pass `memClient` to `queue.NewWorkerPool` and `scheduler.Start`.
  7. In `CreateReloadConfigFunc`: re-register sensitive tokens into sanitizer and reload agy/mcp settings dynamically.
- Zero calls to `os.Getenv` or `config.GetEnv` in `main.go` or sub-packages.

### 3.9 `scripts/verify.sh` & `scripts/check-coverage.sh`: Cross-Platform Clean Room
- Implement `run_clean_test`:
  - Toolchain: `PATH`, `HOME`, `GOROOT`, `GOPATH`, `GOCACHE`.
  - Windows runtime: `SystemRoot`, `SYSTEMROOT`, `USERPROFILE`, `TMP`, `TEMP`, `LOCALAPPDATA`, `APPDATA`, `COMSPEC`, `PATHEXT`.
  - Unix: `TMPDIR`, `LANG`, `LC_ALL`.
  - Git isolation: `GIT_CONFIG_NOSYSTEM=1`, `GIT_TERMINAL_PROMPT=0`.
  - CGO default: `CGO_ENABLED=${CGO_ENABLED:-0}`.
- In `scripts/check-coverage.sh`: ensure clean-room test execution failures record an immediate blocking violation rather than silently reporting `status="N/A"`.

---

## 4. Verification Plan

1. **Static Analysis & Invariant Verification**:
   - `git grep "os.Getenv" brain/pkg/ | grep -v "brain/pkg/config"` must return 0.
   - `git grep "config.GetEnv" brain/pkg/` must return 0.
   - `git grep "TEST_DATABASE_URL"` must return 0 across the entire repository.
   - `git grep "GetDBPath"` must return 0 across the entire repository.
2. **Automated Unit & Package Tests**:
   - `brain/pkg/config` tests pass with >= 90% coverage.
   - `brain/pkg/db` tests pass with pure SQLite fixtures.
   - `brain/pkg/memory`, `brain/pkg/queue`, `brain/pkg/runner`, `brain/pkg/scheduler`, and `brain/pkg/sanitizer` tests pass with pure constructor injection.
   - `scheduler-mcp` tests pass with pure SQLite fixtures.
3. **Monorepo Clean-Room Test Execution**:
   - Run `sh scripts/verify.sh --full` under dirty ambient environment (`POSTGRES_HOST=postgres POSTGRES_PASSWORD=badpass GITHUB_PAT=fake`). Verification must pass 100%.
   - Run `sh scripts/check-coverage.sh --check` to ensure all coverage thresholds pass.
