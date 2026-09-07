# Design Specification: Hermetic Configuration & Pure Constructor Dependency Injection

**Date**: 2026-09-06  
**Status**: Draft  
**Target Repositories**: `azylman/aerial`  
**Scope**: `brain/pkg/config`, `brain/pkg/db`, `brain/pkg/memory`, `brain/pkg/gitsync`, `brain/pkg/sanitizer`, `brain/main.go`, `scheduler-mcp`, `scripts/verify.sh`, `scripts/check-coverage.sh`.

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
- **Ambient Environment Leakage**: Sub-packages (`pkg/db`, `pkg/memory`, `pkg/gitsync`, `pkg/sanitizer`, `scheduler-mcp`) directly called `os.Getenv` to resolve defaults instead of receiving explicit dependencies from their callers.
- **Fragile Denylists**: Attempting to unset individual environment variables (`env -u DATABASE_URL`) fails whenever new infrastructure variables are introduced.
- **Test Harness Contamination**: Tests probed `TEST_DATABASE_URL` and ambient fallback paths instead of operating hermetically with pure in-memory or temporary resources.

### 1.3 The Solution: Pure Constructor Dependency Injection & Clean-Room Testing
We enforce five architectural invariants across the monorepo:
1. **Zero `os.Getenv` in Sub-packages**: No package in `brain/pkg/*` (except `brain/pkg/config`) and no MCP tool module may read the process environment.
2. **Single Parsing Authority**: `brain/pkg/config` is the sole parser of environment variables and configuration files for `brain`.
3. **Pure Constructor Dependency Injection**: Every package constructor requires its configuration and dependencies explicitly (e.g. `InitDB(dsn string)`, `NewOllamaClient(cfg OllamaConfig)`). If a required parameter is empty, constructors return an immediate error rather than falling back to ambient state.
4. **Zero Environment Variables in Tests**: Tests never read `os.Getenv` or call `t.Setenv`. `TEST_DATABASE_URL` is deleted entirely. Tests pass explicit test values, `filepath.Join(t.TempDir(), "test.db")`, or `":memory:"`.
5. **Harness-Level Clean Room**: Test harnesses (`scripts/verify.sh` and `scripts/check-coverage.sh`) execute `go test` under an `env -i` allowlist containing only essential toolchain variables (`PATH`, `HOME`, `GOROOT`, `GOPATH`, `GOCACHE`, `TMPDIR`, `CGO_ENABLED`).

---

## 2. Architectural Invariants & Guarantees

| Invariant | Description | Enforcement Mechanism |
|---|---|---|
| **I1: Single Parsing Authority** | Only `brain/pkg/config` reads `os.Getenv` for `brain`. `main.go` calls `config.LoadConfig()` and passes typed fields. | Static analysis check (`git grep "os.Getenv" brain/pkg/ \| grep -v "brain/pkg/config"` returns 0). |
| **I2: Pure Constructor DI** | Sub-package constructors require all credentials, connection strings, and endpoints as explicit parameters or typed config structs. | No fallback to `os.Getenv`. `db.InitDB("")` returns an explicit error. |
| **I3: Zero Test Env Usage** | Tests never read `os.Getenv` or mutate env via `t.Setenv`. | No `TEST_DATABASE_URL`. Tests instantiate dependencies with pure test fixtures. |
| **I4: Clean-Room Harness** | `verify.sh` and `check-coverage.sh` run tests using `env -i` allowlists. | No ambient credentials can leak into the test process even if set on the host. |
| **I5: Complete Excision of `TEST_DATABASE_URL`** | `TEST_DATABASE_URL` is completely removed from all source code, tests, and scripts. | `git grep "TEST_DATABASE_URL"` returns 0 across the entire repository. |

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
- **Environment Resolution**:
  - `DatabaseURL`: Parsed from `DATABASE_URL`. If unset, synthesized from `POSTGRES_USER` (default `"aerial"`), `POSTGRES_PASSWORD` (default `"aerial_secure_pass"`), `POSTGRES_HOST` (default `"postgres"`), `POSTGRES_PORT` (default `"5432"`), `POSTGRES_DB` (default `"aerial"`), `POSTGRES_SSLMODE` (default `"disable"`). No `TEST_DATABASE_URL`.
  - `Port`: `PORT` (default `"8080"`).
  - `AgyBin`: `AGY_BIN` (default `"agy"`).
  - `APIKey`: `GEMINI_API_KEY` or `ANTIGRAVITY_API_KEY`.
  - `SystemPrompt`: `SYSTEM_PROMPT`.
  - `DiscordToken`: `DISCORD_TOKEN` or `DISCORD_BOT_TOKEN`.
  - `GitHubPAT`: `GITHUB_PAT`.
  - `Ollama`: `OLLAMA_URL` (default `"http://localhost:11434"`), `EMBEDDING_MODEL` or `OLLAMA_EMBEDDING_MODEL` (default `"nomic-embed-text"`), `EMBEDDING_QUERY_PREFIX` (default `"search_query: "`).
  - `ClassifierModel`: `CLASSIFIER_MODEL` (default `"gemini-2.5-flash"`).
- **Function Refactoring**:
  - `GetTimezone()` and `GetSystemChannel()`: operate solely on `GetRuntimeConfig()`, removing direct `os.Getenv` fallbacks.
  - `EnsureAgySettings(apiKey string, homeDir string)`: accepts explicit API key and home directory.
  - `EnsureMcpConfig(pat string, homeDir string)`: accepts explicit GitHub PAT and home directory.
  - `config_test.go`: All tests pass synthetic YAML or construct structs directly; zero reliance on host environment.

### 3.2 `brain/pkg/db`: Strict Constructor Requirement
- **Delete `GetDBPath()`**: Removed entirely from `brain/pkg/db`.
- **Hardened `InitDB(dsn string)`**:
  ```go
  func InitDB(dsn string) (*sql.DB, error) {
      if strings.TrimSpace(dsn) == "" {
          return nil, fmt.Errorf("db: connection string cannot be empty")
      }
      // ... connection pool setup & schema migrations ...
  }
  ```
- **Hermetic Testing in `db_test.go`**:
  - Delete `TestGetDBPath_Precedence`.
  - In `setupTestDB()`: remove `os.Getenv("TEST_DATABASE_URL")` and fallback to live Postgres. Tests strictly create a temporary SQLite database via `filepath.Join(t.TempDir(), "test.db")` or `":memory:"`.

### 3.3 `brain/pkg/memory`: Pure Client Constructor
- **`OllamaClient` Configuration**:
  ```go
  type OllamaClient struct {
      client *http.Client
      cfg    config.OllamaConfig
  }

  func NewOllamaClient(cfg config.OllamaConfig) *OllamaClient {
      if cfg.BaseURL == "" {
          cfg.BaseURL = "http://localhost:11434"
      }
      if cfg.Model == "" {
          cfg.Model = "nomic-embed-text"
      }
      if cfg.QueryPrefix == "" {
          cfg.QueryPrefix = "search_query: "
      }
      return &OllamaClient{
          client: &http.Client{Timeout: 30 * time.Second},
          cfg:    cfg,
      }
  }
  ```
- **Delete `os.Getenv` in `ollama.go`**: Lines 30, 64, 69, 71 removed.
- **Hermetic Testing in `memory_test.go`**:
  - In `memory_test.go`: remove `os.Getenv("TEST_DATABASE_URL")`. Tests construct `NewOllamaClient(config.OllamaConfig{BaseURL: server.URL, ...})` directly.

### 3.4 `brain/pkg/gitsync`: Explicit Token Injection
- **`SyncRepo` Signature**:
  ```go
  func SyncRepo(ctx context.Context, repoDir string, pat string) error
  ```
- **Delete `os.Getenv("GITHUB_PAT")`**: Token is passed from the caller (`main.go` using `cfg.GitHubPAT`).

### 3.5 `brain/pkg/sanitizer`: Explicit Sensitive Token Registration
- **Token Registration**:
  - Delete iteration over ambient environment variables in `InitSanitizer()`.
  - Provide explicit registration:
    ```go
    func RegisterSensitiveTokens(tokens ...string)
    ```
  - `main.go` invokes `sanitizer.RegisterSensitiveTokens(cfg.APIKey, cfg.DiscordToken, cfg.GitHubPAT)`.

### 3.6 `scheduler-mcp`: Service-Level Isolation
- **Delete `GetDBPath()`**: Removed from `scheduler-mcp/db.go`.
- **Hardened `InitDB(dsn string)`**: Strictly requires non-empty `dsn`.
- **Explicit Config in `main.go`**:
  - `main.go` reads `PORT`, `DATABASE_URL` / `POSTGRES_*`, `DEFAULT_TIMEZONE` / `TZ`.
  - Passes explicit `dsn` to `db.InitDB(dsn)` and explicit `timezone` to tools server.
- **Hermetic Testing**:
  - Remove all `TEST_DATABASE_URL` checks and `GetDBPath` tests from `scheduler-mcp/server_test.go` and `scheduler-mcp/tools_test.go`. All tests use SQLite temp files or `":memory:"`.

### 3.7 `brain/main.go`: Pure Composition Root
- `main.go` acts solely as the composition root:
  1. `cfg, err := config.LoadConfig()`
  2. `database, err := db.InitDB(cfg.DatabaseURL)`
  3. `sanitizer.RegisterSensitiveTokens(cfg.APIKey, cfg.DiscordToken, cfg.GitHubPAT)`
  4. `ollamaClient := memory.NewOllamaClient(cfg.Ollama)`
  5. `config.EnsureAgySettings(cfg.APIKey, homeDir)`
  6. `config.EnsureMcpConfig(cfg.GitHubPAT, homeDir)`
- Zero calls to `os.Getenv` in `main.go` or sub-packages.

### 3.8 `scripts/verify.sh` & `scripts/check-coverage.sh`: Clean-Room Test Execution
- Replace all instances of `env -u DATABASE_URL` with a clean-room execution wrapper:
  ```sh
  run_clean_test() {
      env -i \
          PATH="$PATH" \
          HOME="$HOME" \
          GOROOT="${GOROOT:-}" \
          GOPATH="${GOPATH:-}" \
          GOCACHE="${GOCACHE:-}" \
          TMPDIR="${TMPDIR:-/tmp}" \
          CGO_ENABLED="${CGO_ENABLED:-1}" \
          go test "$@"
  }
  ```
- No ambient environment variables (passwords, tokens, hostnames, ports) can reach `go test`.

---

## 4. Spec Self-Review Checklist

1. **Placeholder scan**: Zero "TBD", "TODO", or vague requirements. All struct fields and constructor signatures are fully specified.
2. **Internal consistency**: Architectural principles match component refactorings. Every package is completely purged of `os.Getenv`.
3. **Scope check**: Well-bounded to environment parsing consolidation, pure constructor injection, test cleanup, and harness isolation across `brain` and `scheduler-mcp`.
4. **Ambiguity check**: Clear error returned (`"db: connection string cannot be empty"`) if an empty DSN is provided. Clear `env -i` allowlist specified for test scripts.

---

## 5. Verification Plan

1. **Static Analysis & Invariant Verification**:
   - `git grep "os.Getenv" brain/pkg/ | grep -v "brain/pkg/config"` must return 0.
   - `git grep "TEST_DATABASE_URL"` must return 0 across the entire repository.
   - `git grep "GetDBPath"` must return 0 across the entire repository.
2. **Automated Unit & Package Tests**:
   - `brain/pkg/config` tests pass with >= 90% coverage.
   - `brain/pkg/db` tests pass with pure SQLite fixtures.
   - `brain/pkg/memory`, `brain/pkg/queue`, `brain/pkg/runner`, and `brain/pkg/sanitizer` tests pass with pure constructor injection.
   - `scheduler-mcp` tests pass with pure SQLite fixtures.
3. **Monorepo Clean-Room Test Execution**:
   - Run `sh scripts/verify.sh --full` under dirty ambient environment (e.g. `POSTGRES_HOST=postgres POSTGRES_PASSWORD=badpass GITHUB_PAT=fake`). Verification must pass 100%.
   - Run `sh scripts/check-coverage.sh --check` to ensure monorepo coverage remains >= 90%.
