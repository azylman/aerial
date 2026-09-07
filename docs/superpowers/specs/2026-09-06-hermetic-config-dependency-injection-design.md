# Design Specification: Hermetic Configuration & Pure Pointer Dependency Injection

**Date**: 2026-09-06  
**Status**: Revised & Hardened (Post 4-Expert Adversarial & Systems Audit)  
**Target Repositories**: `azylman/aerial`  
**Scope**: `brain/pkg/config`, `brain/pkg/db`, `brain/pkg/memory`, `brain/pkg/gitsync`, `brain/pkg/sanitizer`, `brain/pkg/scheduler`, `brain/pkg/queue`, `brain/pkg/classifier`, `brain/pkg/skills`, `brain/main.go`, `scheduler-mcp`, `scripts/verify.sh`, `scripts/check-coverage.sh`, `scripts/migrate_sqlite_to_postgres.go`.

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

### 1.3 The Invariant Solution: `cfg.Current()` Atomic Snapshot & Zero-Accessor Invariant
We enforce eight non-negotiable architectural invariants across the monorepo:
1. **Universal Pointer Constructor Injection**: Every package other than `config` (`pkg/db`, `pkg/memory`, `pkg/queue`, `pkg/classifier`, `pkg/scheduler`, `pkg/sanitizer`, `pkg/gitsync`, `pkg/skills`, `scheduler-mcp`) must receive a pointer to a config struct (`*config.Config`) in its constructor.
2. **Atomic Snapshot with Zero Field Accessors**: `config.Config` wraps an `atomic.Pointer[ConfigData]`. Sub-packages call `c := cfg.Current()` to obtain an immutable snapshot pointer. `ConfigData` is a pure, dumb data struct with **zero methods and zero field accessors**. Sub-packages access raw fields directly (`c.Model`, `c.Timezone`, `c.DatabaseURL`). Because `ConfigData` has zero methods, it is **physically impossible** for an accessor to serve as a backdoor to `os.Getenv`.
3. **Nil Safety Guarantee**: `cfg.Current()` is mathematically guaranteed never to return `nil`. If the receiver is `nil` or the atomic pointer is uninitialized, it returns a safe, frozen `DefaultConfigData()` snapshot.
4. **Defensive Ingestion-Time Cloning**: To prevent fatal Go runtime concurrent map read/write crashes (`fatal error: concurrent map read and map write`) and slice backing-array corruption, `Config.Update` and `NewTestConfig` deep-clone all reference fields (`Channels`, `AdminUsers`, `McpServers`, `GitSync.Repositories`).
5. **Zero Ambient Getters & Zero Backdoors**: `config.GetEnv` is deleted and unexported (`getEnv` private to `pkg/config`). No sub-package ever calls `os.Getenv` or `config.GetEnv`.
6. **Zero Stale Field Caching (Invariant I5)**: Sub-packages store ONLY `cfg *config.Config`. They must NEVER copy or cache scalar fields (such as `Model`, `Timezone`, `Channels`, `SystemPrompt`, or `Ollama`) into long-lived struct fields during construction. All dynamic configuration MUST be read just-in-time via `c := cfg.Current()` at the exact moment of execution (e.g. turn execution, retry attempts, cron evaluation, embedding generation).
7. **Stateless Channel Policy & Admin Resolution**: Channel policy resolution (snowflake matching, `#name` matching, cascading default inheritance) is preserved as a pure, stateless package helper: `config.ResolveChannelPolicy(c.Channels, id, name)` and `config.IsAdmin(c.AdminUsers, id, name, globalName)`.
8. **Hermetic Clean-Room Testing Without Env**: Unit tests instantiate `cfg := config.NewTestConfig(&config.ConfigData{ DatabaseURL: ":memory:", Timezone: "UTC" })` and pass `cfg` directly into constructors. `TEST_DATABASE_URL` is 100% excised from the repository. Test harnesses use an OS-aware clean-room allowlist.

---

## 2. Detailed Component Architecture

### 2.1 `brain/pkg/config`: Single Source of Truth & Atomic Hot Reload

#### 2.1.1 Dumb Data Struct `ConfigData` & Atomic Holder `Config`
```go
package config

import (
	"encoding/json"
	"log"
	"strings"
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

var defaultConfigSnapshot *ConfigData

func init() {
	defaultIgnoreBots := true
	defaultConfigSnapshot = &ConfigData{
		Model:         "Gemini 3.6 Flash (Low)",
		Timezone:      "America/Los_Angeles",
		SystemChannel: "aerial-dev",
		AdminUsers:    []string{},
		Channels: map[string]ChannelPolicy{
			"default": {
				Mode:       "threads",
				IgnoreBots: &defaultIgnoreBots,
			},
		},
		GitSync: GitSyncConfig{
			Enabled:       true,
			Interval:      "60s",
			ConfigRepoUrl: "https://github.com/azylman/aerial-config.git",
			Repositories:  []string{"/share/aerial-config", "/share/aerial"},
		},
		McpServers:      make(map[string]json.RawMessage),
		Port:            "8080",
		AgyBin:          "agy",
		ClassifierModel: "Gemini 3.8 Flash (Low)",
	}
}

func DefaultConfigData() *ConfigData {
	return cloneConfigData(defaultConfigSnapshot)
}

func cloneChannelPolicy(p ChannelPolicy) ChannelPolicy {
	cp := p
	if p.IgnoreBots != nil {
		b := *p.IgnoreBots
		cp.IgnoreBots = &b
	}
	if p.AmbientWakeThreshold != nil {
		f := *p.AmbientWakeThreshold
		cp.AmbientWakeThreshold = &f
	}
	return cp
}

func cloneConfigData(src *ConfigData) *ConfigData {
	if src == nil {
		return nil
	}
	dst := *src // Shallow copy scalars

	if src.AdminUsers != nil {
		dst.AdminUsers = make([]string, len(src.AdminUsers))
		copy(dst.AdminUsers, src.AdminUsers)
	}
	if src.Channels != nil {
		dst.Channels = make(map[string]ChannelPolicy, len(src.Channels))
		for k, v := range src.Channels {
			dst.Channels[k] = cloneChannelPolicy(v) // Deep copy pointers
		}
	}
	if src.GitSync.Repositories != nil {
		dst.GitSync.Repositories = make([]string, len(src.GitSync.Repositories))
		copy(dst.GitSync.Repositories, src.GitSync.Repositories)
	}
	if src.McpServers != nil {
		dst.McpServers = make(map[string]json.RawMessage, len(src.McpServers))
		for k, v := range src.McpServers {
			dst.McpServers[k] = append(json.RawMessage(nil), v...)
		}
	}
	return &dst
}

// Current returns the active, immutable ConfigData snapshot. Guaranteed non-nil.
func (c *Config) Current() *ConfigData {
	if c == nil {
		return DefaultConfigData()
	}
	cur := c.current.Load()
	if cur == nil {
		return DefaultConfigData()
	}
	return cur
}

// update atomically replaces the current configuration snapshot.
// Unexported to enforce strict read-only access for sub-packages.
// Only called internally by Reload().
func (c *Config) update(fresh *ConfigData) {
	if fresh == nil {
		return
	}
	c.current.Store(cloneConfigData(fresh))
}

// New constructs a *Config wrapping the given ConfigData.
// Used for hermetic unit testing and production initialization.
func New(data *ConfigData) *Config {
	if data == nil {
		data = DefaultConfigData()
	}
	cfg := &Config{}
	cfg.current.Store(cloneConfigData(data))
	return cfg
}

// Reload parses configuration from search paths and atomically updates the active Config in-place.
func Reload(activeCfg *Config) error {
	freshCfg, err := LoadConfig()
	if err != nil {
		return err
	}
	activeCfg.Update(freshCfg.Current())
	return nil
}
```

#### 2.1.2 Stateless Channel Policy & Admin Resolution
Because `ConfigData` has zero methods, resolution logic is exported as pure stateless package functions:
```go
func ResolveChannelPolicy(channels map[string]ChannelPolicy, channelID, channelName string) ChannelPolicy {
	// Full 60-line logic preserving snowflake lookup, name normalization, and default cascading inheritance
	...
}

func IsAdmin(adminUsers []string, identifiers ...string) bool {
	...
}
```

#### 2.1.3 Safe Environment Resolution & Resilient Cold-Start
- `buildPostgresDSNFromEnv()`:
  - If `DATABASE_URL` is set, return it.
  - If `POSTGRES_HOST` is set, synthesize connection string via `net.JoinHostPort(dbHost, dbPort)`.
  - If `POSTGRES_HOST` is unset, **return empty string `""`**. Never default to `postgres:5432`.
- `LoadConfigFromPaths()`: continues searching candidate paths (including on-disk `/data/.config.yaml.lkgc`) if earlier files encounter YAML syntax or validation errors.
- `LoadConfig()`: returns `(*Config, error)`.
- `getEnv`: made private unexported function. `config.GetEnv` is deleted.

---

### 2.2 `brain/pkg/db`: Universal `New` Constructor, Private `initDB`, Driver Invariants
- **Public Constructor Signature**:
  ```go
  func New(cfg *config.Config) (*sql.DB, error)
  ```
- **Private Helper**:
  ```go
  func initDB(dsn string) (*sql.DB, error) {
      return New(config.New(&config.ConfigData{DatabaseURL: dsn}))
  }
  ```
- **Validation**:
  - If `cfg == nil`, return `fmt.Errorf("db: config cannot be nil")`.
  - `cur := cfg.Current()`
  - `trimmed := strings.TrimSpace(cur.DatabaseURL)`
  - If `trimmed == ""`, return `fmt.Errorf("db: connection string cannot be empty")`.
  - Normalization: if `strings.HasPrefix(trimmed, "sqlite://")`, strip prefix to yield standard file path.
  - Rejection: if `strings.Contains(trimmed, "://") && !isPg && !strings.HasPrefix(trimmed, "file://")`, return `fmt.Errorf("db: unsupported database scheme in %q", trimmed)`.
  - SQLite: if `trimmed == ":memory:" || strings.Contains(trimmed, "mode=memory")`, enforce `database.SetMaxOpenConns(1)`.
  - PostgreSQL: acquires `SELECT pg_advisory_lock(849201948201)` via dedicated `*sql.Conn` during migrations.
- **Delete `GetDBPath()`**: Removed completely.
- **Hermetic Testing**: All unit tests use `config.New(&config.ConfigData{ DatabaseURL: filepath.Join(t.TempDir(), "test.db") })` or `:memory:`. Zero `os.Getenv` or `TEST_DATABASE_URL`.

---

### 2.3 `brain/pkg/memory`: Pure Constructor Injection (`memory.New`)
- **Constructor Signature**:
  ```go
  type Client struct {
      cfg        *config.Config
      httpClient *http.Client
  }

  func New(cfg *config.Config) *Client
  ```
- **Execution**: Inside `EmbedQuery` and `EmbedDocuments`, reads `cur := c.cfg.Current(); cur.Ollama.BaseURL, cur.Ollama.Model, cur.Ollama.QueryPrefix` JIT.
- **Zero `os.Getenv`**: Lines 30, 64, 69, 71 in `ollama.go` removed entirely.

---

### 2.4 `brain/pkg/classifier`: Pure Constructor Injection (`classifier.New`)
- **Constructor Signature**:
  ```go
  type Classifier struct {
      cfg      *config.Config
      runnerFn runner.RunnerFunc
  }

  func New(cfg *config.Config, runnerFn runner.RunnerFunc) *Classifier
  ```
- In `Classify(ctx, text)`: evaluates `cur := c.cfg.Current()` JIT to obtain `cur.ClassifierModel`, `cur.AgyBin`, `cur.APIKey`. Adheres strictly to Invariant I5.

---

### 2.5 `brain/pkg/sanitizer`: Copy-On-Write Token Registration
- **Constructor / Registration**:
  ```go
  func RegisterConfigTokens(cfg *config.Config)
  ```
- In `RegisterConfigTokens`: reads `cur := cfg.Current()`. Uses a thread-safe Copy-On-Write slice via `atomic.Pointer[[]string]` to eliminate reader data races. Deduplicates against existing secrets before appending `cur.APIKey`, `cur.DiscordToken`, `cur.GitHubPAT`, and database passwords. Capped at 256 secrets.
- Removes automatic scan of `os.Environ()` and `os.Getenv`.

---

### 2.6 `brain/pkg/queue`: Constructor Separation (`queue.New`) & Retry Re-evaluation
- **Constructor Signature**:
  ```go
  func New(appCfg *config.Config, cfg WorkerPoolConfig) *WorkerPool
  ```
- **WorkerPool Struct Definition**:
  ```go
  type WorkerPool struct {
      appCfg    *config.Config         // Pure application config pointer
      cfg       WorkerPoolConfig       // Non-config dependencies (DB, Classifier, Hooks)
      threadChs map[string]*threadWorkerState
      ...
  }
  ```
  `WorkerPoolConfig` retains internal infrastructure dependencies (`DB`, `Classifier`, `DeliveryFunc`, `RunnerFunc`, `Metrics`, `ResolveChannelPolicy`), but **removes** `Model`, `AgyBin`, `APIKey`, and `SystemPrompt`.
- In `processBurst`: evaluates `cur := p.appCfg.Current()` at burst entry AND re-evaluates `cur = p.appCfg.Current()` at the top of each retry attempt in the loop.
- Resolves policy via `config.ResolveChannelPolicy(cur.Channels, effectiveID, effectiveName)`. If `p.cfg.ResolveChannelPolicy != nil`, uses it as an optional test override hook.
- Deletes `WorkerPool.UpdateRuntimeConfig(...)`.

---

### 2.7 `brain/pkg/scheduler`: Explicit Service Struct (`scheduler.New`)
- **Scheduler Struct & Constructor**:
  ```go
  type Scheduler struct {
      cfg           *config.Config
      db            *sql.DB
      pool          *queue.WorkerPool
      dgSession     *discordgo.Session
      threadCreator ThreadCreator
      memClient     *memory.Client
  }

  func New(cfg *config.Config, db *sql.DB, pool *queue.WorkerPool, dgSession *discordgo.Session) *Scheduler

  func NewScheduler(cfg *config.Config, db *sql.DB, pool *queue.WorkerPool, dgSession *discordgo.Session, memClient *memory.Client) *Scheduler
  ```
- In `s.ProcessDueSchedules`: reads `cur := s.cfg.Current(); cur.Timezone, cur.Channels`.
- In `s.ExtractFactsLLM`: reads `cur := s.cfg.Current(); cur.AgyBin, cur.APIKey, cur.Model`.
- Eliminates all calls to `config.GetEnv`, `config.GetTimezone()`, and `config.GetRuntimeConfig()`.

---

### 2.8 `scheduler-mcp`: Service-Level Constructor Parity
- Defines `type Config struct { DatabaseURL string; Timezone string; Port string }`.
- `InitDB(cfg *Config) (*sql.DB, error)` requires non-empty `DatabaseURL`. Sets `SetMaxOpenConns(1)` for `:memory:`. Acquires `SELECT pg_advisory_lock(849201948201)` via dedicated connection during PostgreSQL migrations.
- `NewToolHandler(cfg *Config, database *sql.DB) *ToolHandler`.
- Removes `GetDBPath()` and all ambient `os.Getenv` from `db.go`, `tools.go`, and `server_test.go`.

---

### 2.9 `brain/main.go`: Pure Composition Root with Serialized Hot-Reload
- Guarded `reloadConfig`:
  ```go
  var reloadMu sync.Mutex

  reloadConfig := func(source string) {
      reloadMu.Lock()
      defer reloadMu.Unlock()

      log.Printf("[%s] Hot reload triggered...", source)
      if err := config.Reload(cfg); err != nil {
          log.Printf("[%s] Warning: config reload error: %v (retaining LKGC)", source, err)
          if dgSession != nil {
              _ = delivery.SendSystemAlert(dgSession, cfg.Current().SystemChannel, "Invalid Configuration File", err.Error())
          }
          return // Early exit protects on-disk LKGC state from corruption
      }

      sanitizer.RegisterConfigTokens(cfg)
      cur := cfg.Current()
      _ = config.EnsureAgySettings(cur.APIKey, cur.Model)
      _ = config.EnsureMcpConfig(cur.McpServers)
      _ = config.EnsureSystemRules(cur.SystemPrompt)
      _ = skills.EnsureSkills()
  }
  ```

---

### 2.10 `scripts/verify.sh` & `scripts/check-coverage.sh`: Non-Empty Allowlist
- `run_clean_test` uses POSIX parameter expansion `${VAR:+VAR="$VAR"}`:
  ```bash
  run_clean_test() {
      env -i \
          ${PATH:+PATH="$PATH"} \
          ${HOME:+HOME="$HOME"} \
          ${GOROOT:+GOROOT="$GOROOT"} \
          ${GOPATH:+GOPATH="$GOPATH"} \
          ${GOCACHE:+GOCACHE="$GOCACHE"} \
          ${TMPDIR:+TMPDIR="$TMPDIR"} \
          ${SystemRoot:+SystemRoot="$SystemRoot"} \
          ${SYSTEMROOT:+SYSTEMROOT="$SYSTEMROOT"} \
          ${USERPROFILE:+USERPROFILE="$USERPROFILE"} \
          ${HOMEDRIVE:+HOMEDRIVE="$HOMEDRIVE"} \
          ${HOMEPATH:+HOMEPATH="$HOMEPATH"} \
          ${TMP:+TMP="$TMP"} \
          ${TEMP:+TEMP="$TEMP"} \
          ${LOCALAPPDATA:+LOCALAPPDATA="$LOCALAPPDATA"} \
          ${APPDATA:+APPDATA="$APPDATA"} \
          ${COMSPEC:+COMSPEC="$COMSPEC"} \
          ${PATHEXT:+PATHEXT="$PATHEXT"} \
          MSYS_NO_PATHCONV=1 \
          LANG="${LANG:-en_US.UTF-8}" \
          LC_ALL="${LC_ALL:-en_US.UTF-8}" \
          GIT_CONFIG_NOSYSTEM=1 \
          GIT_TERMINAL_PROMPT=0 \
          CGO_ENABLED="${CGO_ENABLED:-0}" \
          go test "$@"
  }
  ```
- In `scripts/check-coverage.sh`: remove `|| true` and `>/dev/null 2>&1`; record any test exit failure as a blocking violation.

---

## 3. Verification Plan

1. **Static Invariant Checks**:
   - `test $(git grep -E "os\.(Getenv|LookupEnv|Environ|ExpandEnv)" brain/pkg/ | grep -v "brain/pkg/config" | wc -l) -eq 0`
   - `test $(git grep "config.GetEnv" brain/ | wc -l) -eq 0`
   - `test $(git grep -I "TEST_DATABASE_URL" -- ':(exclude)docs/' ':(exclude)*.md' | wc -l) -eq 0`
   - `test $(git grep -I "GetDBPath" -- ':(exclude)docs/' ':(exclude)*.md' | wc -l) -eq 0`
2. **Race Detector Validation**:
   - `go test -race -v -count=5 ./brain/pkg/config`
   - `go test -race -v -count=5 ./brain/pkg/queue`
   - `go test -race -v -count=5 ./brain/pkg/scheduler`
3. **Dirty Clean-Room Verification**:
   - `POSTGRES_HOST=postgres POSTGRES_PASSWORD=badpass GITHUB_PAT=fake sh scripts/verify.sh --full`
