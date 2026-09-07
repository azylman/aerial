# Design Specification: Hermetic Configuration & Pure Pointer Dependency Injection

**Date**: 2026-09-06  
**Status**: Revised (Aligned with `cfg.Current()` Atomic Snapshot & Zero-Accessor Invariant)  
**Target Repositories**: `azylman/aerial`  
**Scope**: `brain/pkg/config`, `brain/pkg/db`, `brain/pkg/memory`, `brain/pkg/gitsync`, `brain/pkg/sanitizer`, `brain/pkg/scheduler`, `brain/pkg/queue`, `brain/main.go`, `scheduler-mcp`, `scripts/verify.sh`, `scripts/check-coverage.sh`.

---

## 1. Executive Summary & Problem Statement

### 1.1 The Incident & Root Cause
During automated PR submission (`aerial-pr.sh submit`), a false-alarm "poison pill" alert was dispatched to Discord for an in-flight message. Thorough post-incident diagnosis revealed that the production runtime never crashed. Instead:
1. `go test` executed inside the production container during pre-commit validation.
2. The test runner script (`scripts/verify.sh`) attempted to isolate tests by unsetting `DATABASE_URL` via `env -u DATABASE_URL`.
3. However, `POSTGRES_HOST=postgres` and associated `POSTGRES_*` environment variables remained set in the container environment.
4. When unit tests invoked database initialization or test fixtures, `pkg/db.GetDBPath()` inspected the ambient environment, fell back to `POSTGRES_*`, and connected directly to the **live production PostgreSQL database**.
5. A test queue processor found the production in-flight message in `StatusProcessing`, incremented its retry count to 3, and dispatched a poison-pill alert to Discord.

### 1.2 The Architectural Flaw: Ambient Environment Coupling & Backdoors
The root failure was not a missing flag in a test script; it was architectural:
- **Ambient Environment Leakage**: Sub-packages (`pkg/db`, `pkg/memory`, `pkg/gitsync`, `pkg/sanitizer`, `scheduler-mcp`, `pkg/scheduler`) directly called `os.Getenv` or `config.GetEnv` to resolve defaults instead of receiving explicit dependencies from their callers.
- **Backdoor Getters**: `config.GetEnv(...)` was imported by sub-packages (e.g. `brain/pkg/scheduler/scheduler.go:312`), and `config.GetTimezone()` secretly fell back to `os.Getenv("DEFAULT_TIMEZONE")`.
- **Fragile Denylists**: Attempting to unset individual environment variables (`env -u DATABASE_URL`) fails whenever new infrastructure variables are introduced.
- **Test Harness Contamination**: Tests probed `TEST_DATABASE_URL` and ambient fallback paths instead of operating hermetically with pure in-memory or temporary resources.
- **Toxic Defaults**: When `POSTGRES_HOST` was unset, code automatically generated connection strings pointing to `postgres:5432`, creating startup hangs (~27.5s) on non-Docker local environments.

### 1.3 The Invariant Solution: `cfg.Current()` Atomic Snapshot & Zero-Accessor Pattern
We enforce five non-negotiable architectural invariants across the monorepo:
1. **Universal Pointer Constructor Injection**: Every package other than `config` (`pkg/db`, `pkg/memory`, `pkg/queue`, `pkg/runner`, `pkg/scheduler`, `pkg/sanitizer`, `pkg/gitsync`, `pkg/session`, `pkg/skills`, `pkg/watcher`) must receive a pointer to a config struct (`*config.Config`) in its constructor.
2. **Atomic Snapshot with Zero Field Accessors**: `config.Config` wraps an `atomic.Pointer[ConfigData]`. Sub-packages call `c := cfg.Current()` to obtain an immutable snapshot pointer. `ConfigData` is a pure, dumb data struct with **zero methods and zero field accessors**. Sub-packages access raw fields directly (`c.Model`, `c.Timezone`, `c.DatabaseURL`). Because `ConfigData` has zero methods, it is **physically impossible** for an accessor to serve as a backdoor to `os.Getenv`.
3. **Zero Ambient Getters & Zero Backdoors**: `config.GetEnv` is deleted and unexported (`getEnv` private to `pkg/config`). No sub-package ever calls `os.Getenv` or `config.GetEnv`.
4. **Hardware Atomic Hot Reload**: When configuration reloads (`config.Reload(activeCfg)`), the `config` package parses the new configuration and performs a lock-free atomic pointer swap `activeCfg.Update(freshData)`. All holding packages immediately and safely observe the new snapshot on subsequent `cfg.Current()` calls without torn reads, data races, or manual cross-package setters.
5. **Hermetic Testing Without Environment Variables**: Unit tests instantiate `cfg := config.NewTestConfig(&config.ConfigData{ DatabaseURL: ":memory:", Timezone: "UTC" })` and pass `cfg` directly into constructors. Tests never touch `os.Getenv` or `t.Setenv`. `TEST_DATABASE_URL` is excised from the entire repository.

---

## 2. Architectural Invariants & Guarantees

| Invariant | Description | Enforcement Mechanism |
|---|---|---|
| **I1: Single Parsing Authority** | Only `brain/pkg/config` reads `os.Getenv` for `brain`. Subpackages cannot call `config.GetEnv` (unexported/deleted). `main.go` calls `config.LoadConfig()` to instantiate the root `*config.Config`. | Static analysis check (`git grep "os.Getenv" brain/pkg/ \| grep -v "brain/pkg/config"` returns 0, and `git grep "config.GetEnv" brain/` returns 0). |
| **I2: Zero Field Accessors** | `ConfigData` has zero methods. Subpackages read pure fields directly from `cfg.Current()`. No field-level getters exist. | Code structure: `ConfigData` contains only public data fields. No getters (`GetModel`, `GetTimezone`) exist to act as backdoors. |
| **I3: Pure `*config.Config` Constructor DI** | Sub-package constructors take `cfg *config.Config`. Their only interaction with `config` is reading through this pointer. | Constructors require `cfg *config.Config`. `db.InitDB(cfg)` returns error if `cfg.Current().DatabaseURL` is empty. |
| **I4: Atomic Hot Reload via `cfg.Current()`** | `config.Config` wraps `atomic.Pointer[ConfigData]`. `cfg.Update(fresh)` executes an atomic pointer swap (1 CPU instruction). | Race detector validation (`go test -race ./brain/pkg/...`). Zero lock contention, zero torn reads. |
| **I5: Zero Test Env Usage for Infrastructure** | Tests never read `os.Getenv` or mutate env via `t.Setenv` for credentials or infrastructure. | No `TEST_DATABASE_URL`. Tests instantiate `config.NewTestConfig(&config.ConfigData{ DatabaseURL: ":memory:" })` directly. |
| **I6: Clean-Room Test Harness** | `verify.sh` and `check-coverage.sh` run tests using OS-aware clean-room allowlists. | Ambient credentials cannot leak into test processes even if host environment contains `POSTGRES_HOST=postgres`. |
| **I7: Complete Excision of `TEST_DATABASE_URL`** | `TEST_DATABASE_URL` is completely removed from all source code, tests, scripts, and workflows. | `git grep "TEST_DATABASE_URL"` returns 0 across the entire repository. |

---

## 3. Detailed Component Architecture

### 3.1 `brain/pkg/config`: Single Source of Truth & Atomic Hot Reload
`brain/pkg/config` is the sole parser of environment variables and configuration files for `brain`.

#### 3.1.1 Dumb Data Struct `ConfigData` & Atomic Holder `Config`
```go
package config

import (
	"encoding/json"
	"sync/atomic"
)

// ConfigData is a 100% pure, dumb data struct.
// ZERO methods. ZERO field accessors. ZERO dynamic logic.
// It is physically impossible for this struct to call os.Getenv.
type ConfigData struct {
	// Runtime / Policy Settings (from config.yaml / options.json)
	Model         string                     `yaml:"model" json:"model"`
	Timezone      string                     `yaml:"timezone" json:"timezone"`
	SystemChannel string                     `yaml:"system_channel" json:"system_channel"`
	AdminUsers    []string                   `yaml:"admin_users" json:"admin_users"`
	Channels      map[string]ChannelPolicy   `yaml:"channels" json:"channels"`
	GitSync       GitSyncConfig              `yaml:"git_sync" json:"git_sync"`
	McpServers    map[string]json.RawMessage `yaml:"mcp_servers,omitempty" json:"mcp_servers,omitempty"`

	// Infrastructure & Environment Settings (parsed strictly by pkg/config)
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

// Config wraps an atomic pointer to the active ConfigData snapshot.
type Config struct {
	current atomic.Pointer[ConfigData]
}

// NewTestConfig instantiates a Config pointer directly with test data.
func NewTestConfig(data *ConfigData) *Config {
	c := &Config{}
	c.current.Store(data)
	return c
}

// Current returns the active, immutable snapshot. Subpackages read raw fields directly.
func (c *Config) Current() *ConfigData {
	return c.current.Load()
}

// Update atomically replaces the active configuration snapshot in 1 CPU instruction.
func (c *Config) Update(fresh *ConfigData) {
	c.current.Store(fresh)
}

// Reload parses configuration from search paths and atomically updates the active Config in-place.
// If parsing fails, it preserves Last Known Good Configuration (LKGC) and returns the error.
func Reload(activeCfg *Config) error {
	freshCfg, err := LoadConfig()
	if err != nil {
		return err
	}
	activeCfg.Update(freshCfg.Current())
	return nil
}
```

#### 3.1.2 Environment Parsing & Postgres Default Trap Removal
- `buildPostgresDSNFromEnv()`:
  - If `DATABASE_URL` is set, return it.
  - If `POSTGRES_HOST` is set, synthesize connection string via `net.JoinHostPort(dbHost, dbPort)`.
  - If `POSTGRES_HOST` is unset, **return empty string `""`**. Never default to `postgres:5432`.
- `LoadConfig()`: returns `(*Config, error)`.
- `getEnv`: made private unexported function. `config.GetEnv` is deleted.

---

### 3.2 `brain/pkg/db`: Strict Constructor Requirement & Driver Invariants
- **Constructor Signature**:
  ```go
  func InitDB(cfg *config.Config) (*sql.DB, error)
  ```
- **Validation**:
  - If `cfg == nil`, return `fmt.Errorf("db: config cannot be nil")`.
  - `c := cfg.Current()`
  - `dsn := strings.TrimSpace(c.DatabaseURL)`
  - If `dsn == ""`, return `fmt.Errorf("db: connection string cannot be empty")`.
  - Rejects unrecognized URL schemes (e.g. `mysql://`).
  - Sets `database.SetMaxOpenConns(1)` for in-memory SQLite (`:memory:` or `mode=memory`).
  - Acquires `SELECT pg_advisory_lock(849201948201)` during PostgreSQL migrations.
- **Delete `GetDBPath()`**: Removed completely.
- **Hermetic Testing**: Tests in `db_test.go` instantiate `config.NewTestConfig(&config.ConfigData{ DatabaseURL: filepath.Join(t.TempDir(), "test.db") })` or `&config.ConfigData{ DatabaseURL: ":memory:" }`. Zero `os.Getenv` or `TEST_DATABASE_URL`.

---

### 3.3 `brain/pkg/memory`: Constructor Takes `*config.Config`
- **Constructor Signature**:
  ```go
  type Client struct {
      cfg        *config.Config
      httpClient *http.Client
  }

  func NewClient(cfg *config.Config) *Client
  ```
- **Execution**: When generating embeddings or querying Ollama, reads `c := client.cfg.Current()`, uses `c.Ollama.BaseURL`, `c.Ollama.Model`, `c.Ollama.QueryPrefix`.
- **Zero `os.Getenv`**: Lines 30, 64, 69, 71 in `ollama.go` removed entirely.
- **Hermetic Testing**: Tests in `memory_test.go` pass `config.NewTestConfig(&config.ConfigData{ Ollama: config.OllamaConfig{ BaseURL: server.URL, ... } })`.

---

### 3.4 `brain/pkg/sanitizer`: Sensitive Tokens via `*config.Config`
- **Constructor / Registration**:
  ```go
  func RegisterConfigTokens(cfg *config.Config)
  ```
- Reads `c := cfg.Current()`, appends `c.APIKey`, `c.DiscordToken`, `c.GitHubPAT` to `envSecretsCache`.
- Provides `ResetSensitiveTokens()` for unit test isolation.
- Removes automatic scan of `os.Environ()` and `os.Getenv`.

---

### 3.5 `brain/pkg/gitsync`: Token via `*config.Config`
- **Signature**:
  ```go
  func SyncRepo(ctx context.Context, repoPath string, cfg *config.Config) (bool, error)
  ```
- Reads `c := cfg.Current(); pat := c.GitHubPAT`.
- Deletes `os.Getenv("GITHUB_PAT")`.

---

### 3.6 `brain/pkg/queue`: Constructor Takes `*config.Config`
- **`WorkerPoolConfig`**:
  ```go
  type WorkerPoolConfig struct {
      Config       *config.Config
      DB           *sql.DB
      Classifier   *classifier.Classifier
      DeliveryFunc DeliveryFunc
      RunnerFunc   runner.RunnerFunc
      Metrics      metrics.Recorder
  }
  ```
- `WorkerPool` stores `p.cfg *config.Config`.
- When running turns: reads `c := p.cfg.Current()`, uses `c.Model`, `c.AgyBin`, `c.APIKey`, `c.SystemPrompt`.
- When resolving policies: reads `c.Channels` and `c.SystemChannel`.
- **Hot Reload Elimination**: Because `p.cfg` is updated atomically in-place by `config.Reload`, `WorkerPool` immediately observes updated model/prompts without needing `pool.UpdateRuntimeConfig(...)`.

---

### 3.7 `brain/pkg/scheduler`: Constructor Takes `*config.Config`
- **Constructor Signature**:
  ```go
  func Start(ctx context.Context, database *sql.DB, pool *queue.WorkerPool, dgSession *discordgo.Session, memClient *memory.Client, cfg *config.Config) func()
  ```
- Stores `cfg *config.Config`.
- In `runSchedulerLoop` and `ExtractFactsLLM`:
  Reads `c := s.cfg.Current()`, uses `c.Timezone`, `c.Model`, `c.APIKey`, `c.AgyBin`.
- **Zero Ambient Calls**: Completely removes calls to `config.GetEnv`, `config.GetTimezone()`, and `config.GetRuntimeConfig()`.

---

### 3.8 `scheduler-mcp`: Service-Level Constructor Injection
- Standalone Go module `scheduler-mcp`.
- Defines `type Config struct { DatabaseURL string; Timezone string; Port string }`.
- `InitDB(cfg *Config) (*sql.DB, error)` requires non-empty `DatabaseURL`. Sets `SetMaxOpenConns(1)` for `:memory:`. Advisory lock for Postgres.
- `NewToolHandler(cfg *Config, database *sql.DB) *ToolHandler`.
- Removes `GetDBPath()` and all ambient `os.Getenv` from `db.go` and `tools.go`.
- Hermetic tests construct `&Config{ DatabaseURL: ":memory:", Timezone: "America/Chicago" }`.

---

### 3.9 `brain/main.go` & `funnel.go`: Pure Composition Root
- `main.go` acts solely as the composition root:
  ```go
  func RunBrainApp(ctx context.Context, cfg *config.Config) error {
      c := cfg.Current()

      // 1. Initialize DB
      database, err := db.InitDB(cfg)
      if err != nil { return err }
      defer database.Close()

      // 2. Initialize Memory client
      memClient := memory.NewClient(cfg)

      // 3. Register sensitive tokens
      sanitizer.RegisterConfigTokens(cfg)

      // 4. Initialize Classifier
      cls := classifier.NewClassifier(
          classifier.WithModel(c.ClassifierModel),
          classifier.WithLLMFunc(classifier.NewAgyLLMFunc(c.AgyBin, c.APIKey, runner.RunAgy)),
      )

      // 5. Initialize WorkerPool with Config pointer
      pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
          Config:     cfg,
          DB:         database,
          Classifier: cls,
      })
      pool.Start()

      // 6. Connect Discord Gateway
      dgSession := connectDiscordFunnel(ctx, database, pool, cfg)

      // 7. Atomic Hot Reload Callback
      reloadConfig := func(source string) {
          if err := config.Reload(cfg); err != nil {
              log.Printf("[%s] Warning: config reload error: %v (retaining LKGC)", source, err)
              if dgSession != nil {
                  _ = delivery.SendSystemAlert(dgSession, cfg.Current().SystemChannel, "Invalid Configuration File", err.Error())
              }
          }
          sanitizer.RegisterConfigTokens(cfg)
          cur := cfg.Current()
          _ = config.EnsureAgySettings(cur.APIKey, cur.Model)
          _ = config.EnsureMcpConfig(cur.GitHubPAT, cur.McpServers)
          _ = config.EnsureSystemRules(cur.SystemPrompt)
          _ = skills.EnsureSkills()
      }

      // 8. Start Scheduler with Config pointer
      stopScheduler := scheduler.Start(ctx, database, pool, dgSession, memClient, cfg)
      defer stopScheduler()
      ...
  }
  ```
- Removes `NewBrainConfigFromEnv` and `BrainConfig` wrapper struct.

---

### 3.10 `scripts/verify.sh` & `scripts/check-coverage.sh`: Cross-Platform Clean Room
- `run_clean_test` executes `go test` with an explicit allowlist:
  - Toolchain: `PATH`, `HOME`, `GOROOT`, `GOPATH`, `GOCACHE`.
  - Windows runtime: `SystemRoot`, `SYSTEMROOT`, `USERPROFILE`, `TMP`, `TEMP`, `LOCALAPPDATA`, `APPDATA`, `COMSPEC`, `PATHEXT`.
  - Unix: `TMPDIR`, `LANG`, `LC_ALL`.
  - Git isolation: `GIT_CONFIG_NOSYSTEM=1`, `GIT_TERMINAL_PROMPT=0`.
  - CGO: `CGO_ENABLED=${CGO_ENABLED:-0}`.
- Prevents ambient container variables (`POSTGRES_HOST=postgres`) from leaking into test processes.

---

## 4. Verification Plan

1. **Static Analysis & Invariant Verification**:
   - `git grep "os.Getenv" brain/pkg/ | grep -v "brain/pkg/config"` must return 0.
   - `git grep "config.GetEnv" brain/` must return 0.
   - `git grep "TEST_DATABASE_URL"` must return 0 across the entire repository.
   - `git grep "GetDBPath"` must return 0 across the entire repository.
2. **Automated Unit & Package Tests**:
   - `brain/pkg/config` tests pass with >= 90% coverage, including concurrent `Update()` race testing.
   - `brain/pkg/db` tests pass with pure SQLite fixtures.
   - `brain/pkg/memory`, `brain/pkg/queue`, `brain/pkg/runner`, `brain/pkg/scheduler`, and `brain/pkg/sanitizer` tests pass with pure constructor injection.
   - `scheduler-mcp` tests pass with pure SQLite fixtures.
3. **Monorepo Clean-Room Test Execution**:
   - Run `sh scripts/verify.sh --full` under dirty ambient environment (`POSTGRES_HOST=postgres POSTGRES_PASSWORD=badpass GITHUB_PAT=fake`). Verification must pass 100%.
   - Run `sh scripts/check-coverage.sh --check` to ensure all coverage thresholds pass.
