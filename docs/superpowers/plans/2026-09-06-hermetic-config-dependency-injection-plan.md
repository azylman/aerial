# Hermetic Configuration & Pure Pointer Dependency Injection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish hermetic configuration parsing and pure `*config.Config` pointer constructor dependency injection across the Aerial monorepo, permanently eliminating ambient environment leakage, fragile `env -u` denylists, backdoor getters, and toxic postgres fallback traps.

**Architecture:** Consolidate all environment variable and config-file parsing into `brain/pkg/config`. `config.Config` wraps an `atomic.Pointer[ConfigData]`, where `ConfigData` is a dumb data struct with **zero methods and zero field accessors**. Every package other than `config` (`pkg/db`, `pkg/memory`, `pkg/queue`, `pkg/classifier`, `pkg/scheduler`, `pkg/sanitizer`, `pkg/gitsync`, `pkg/skills`, `scheduler-mcp`) receives `*config.Config` in its constructor. Subpackages call `c := cfg.Current()` to obtain an immutable, nil-safe snapshot and read pure fields directly (`c.Model`, `c.Timezone`, `c.DatabaseURL`). Reference fields (`Channels`, `AdminUsers`, `McpServers`) are defensively deep-cloned on ingestion. No subpackage may ever call `os.Getenv`, `config.GetEnv`, `LoadDefaultConfig`, or package-level getters. Hot reload performs a lock-free atomic pointer swap `activeCfg.Update(freshData)`, so all components holding `*config.Config` observe live updates safely and immediately without manual cross-package setters. Tests instantiate `config.NewTestConfig(&config.ConfigData{ DatabaseURL: ":memory:", ... })` directly with zero environment variable manipulation.

**Tech Stack:** Go 1.24, modernc.org/sqlite, jackc/pgx/v5, gopkg.in/yaml.v3, POSIX sh test harness.

**Spec:** [`docs/superpowers/specs/2026-09-06-hermetic-config-dependency-injection-design.md`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/docs/superpowers/specs/2026-09-06-hermetic-config-dependency-injection-design.md)

## Global Constraints

- Universal constructor injection: Every package other than `config` must receive `cfg *config.Config` in its constructor.
- Zero field accessors: `ConfigData` has zero methods and zero getters. Subpackages call `cfg.Current()` and read raw fields.
- Nil safety: `cfg.Current()` must never return `nil`; if uninitialized or nil receiver, it returns a frozen default snapshot.
- Reference immutability: `Config.Update` and `NewTestConfig` deep-clone `Channels`, `AdminUsers`, `McpServers`, and `Repositories`.
- Zero stale field caching (Invariant I5): Sub-packages store `*config.Config` only. They must NEVER copy or cache scalar fields (`Model`, `Timezone`, `Channels`, `SystemPrompt`, etc.) into long-lived struct fields during construction. All dynamic configuration must be read JIT via `cfg.Current()` at execution time (including retry attempts).
- Stateless policy helpers: `config.ResolveChannelPolicy(channels, id, name)` and `config.IsAdmin(adminUsers, id, name)` are stateless package functions.
- Zero `os.Getenv` or `os.LookupEnv` in `brain/pkg/db`, `brain/pkg/memory`, `brain/pkg/gitsync`, `brain/pkg/sanitizer`, `brain/pkg/queue`, `brain/pkg/runner`, `brain/pkg/scheduler`, `brain/pkg/session`, or `brain/pkg/watcher`.
- `config.GetEnv` is deleted and unexported (`getEnv` private to `brain/pkg/config`).
- `config.Config` manages thread-safe atomic hot-reloads via `atomic.Pointer[ConfigData]`, `Update(fresh)`, and `Reload(activeCfg)`.
- `TEST_DATABASE_URL` is completely excised from all code, tests, scripts, and workflows.
- `DefaultConfigData()` and `buildPostgresDSNFromEnv()` must never default to hostname `postgres:5432` when `POSTGRES_HOST` is unset.
- Unit tests must never call `os.Getenv` or `t.Setenv` for infrastructure/credentials. (Only `config_test.go` and `dashboard/main_test.go` retain scoped `t.Setenv` to test config file/env fallback parsing).
- Constructors strictly validate required parameters: `db.InitDB(cfg)` returns error if `cfg == nil` or connection string is empty. Unrecognized URL schemes return errors instead of silently falling through to SQLite.
- All Go test suites must compile and pass under clean-room execution at every commit.
- Monorepo Go statement coverage must satisfy all package floors and threshold checks.

---

### Task 1: Refactor `brain/pkg/config` for `cfg.Current()` Atomic Snapshot & Zero Accessors

**Files:**
- Modify: `brain/pkg/config/config.go`
- Test: `brain/pkg/config/config_test.go`

**Interfaces:**
- Consumes: Environment variables (`DATABASE_URL`, `POSTGRES_*`, `PORT`, `AGY_BIN`, `GEMINI_API_KEY`, `ANTIGRAVITY_API_KEY`, `SYSTEM_PROMPT`, `DISCORD_TOKEN`, `DISCORD_BOT_TOKEN`, `GITHUB_PAT`, `OLLAMA_URL`, `EMBEDDING_MODEL`, `OLLAMA_EMBEDDING_MODEL`, `EMBEDDING_QUERY_PREFIX`, `CLASSIFIER_MODEL`, `AMBIENT_CLASSIFIER_MODEL`).
- Produces: `ConfigData` (pure dumb struct, 0 methods), `Config` wrapping `atomic.Pointer[ConfigData]`, `cfg.Current() *ConfigData` (nil-safe), `cfg.Update(fresh *ConfigData)` (deep-cloned), `config.NewTestConfig(data *ConfigData) *Config`, `DefaultConfigData() *ConfigData`, `ResolveChannelPolicy(channels, id, name) ChannelPolicy`, `IsAdmin(adminUsers, ids...) bool`, `Reload(activeCfg *Config) error`. Unexports `getEnv` and deletes `config.GetEnv`.

- [ ] **Step 1: Write the failing test**

Add concurrency, deep-cloning, and nil-safety tests to `brain/pkg/config/config_test.go`:
```go
func TestConfig_NilSafetyAndDeepCloning(t *testing.T) {
	// 1. Nil receiver safety
	var nilCfg *Config
	if nilCfg.Current() == nil {
		t.Fatalf("expected DefaultConfigData on nil receiver, got nil")
	}

	// 2. Uninitialized pointer safety
	emptyCfg := &Config{}
	if emptyCfg.Current() == nil {
		t.Fatalf("expected DefaultConfigData on uninitialized Config, got nil")
	}

	// 3. Deep cloning on NewTestConfig & Update prevents external map mutation race
	origChannels := map[string]ChannelPolicy{
		"default": {Mode: "threads"},
	}
	cfg := NewTestConfig(&ConfigData{
		Model:    "test-model",
		Channels: origChannels,
	})

	// Mutate caller map
	origChannels["default"] = ChannelPolicy{Mode: "ignore"}

	// Snapshot must retain original value
	if cfg.Current().Channels["default"].Mode != "threads" {
		t.Errorf("expected deep-cloned snapshot to resist external map mutation, got %q", cfg.Current().Channels["default"].Mode)
	}
}

func TestConfig_PostgresHostUnset_ReturnsEmptyDSN(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("POSTGRES_HOST", "")
	dsn := buildPostgresDSNFromEnv()
	if dsn != "" {
		t.Errorf("expected empty DSN when POSTGRES_HOST is unset, got %q", dsn)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run "TestConfig_NilSafetyAndDeepCloning|TestConfig_PostgresHostUnset_ReturnsEmptyDSN" ./brain/pkg/config`
Expected: FAIL due to undefined functions/types.

- [ ] **Step 3: Implement `ConfigData`, `Config`, `cloneConfigData`, nil-safe `Current()`, `Update()`, and safe defaults**

In `brain/pkg/config/config.go`:
1. Define `ConfigData` as dumb data struct without methods:
```go
type ConfigData struct {
	Model           string                     `yaml:"model" json:"model"`
	Timezone        string                     `yaml:"timezone" json:"timezone"`
	SystemChannel   string                     `yaml:"system_channel" json:"system_channel"`
	AdminUsers      []string                   `yaml:"admin_users" json:"admin_users"`
	Channels      map[string]ChannelPolicy   `yaml:"channels" json:"channels"`
	GitSync       GitSyncConfig              `yaml:"git_sync" json:"git_sync"`
	McpServers    map[string]json.RawMessage `yaml:"mcp_servers,omitempty" json:"mcp_servers,omitempty"`
	DatabaseURL     string                     `yaml:"database_url" json:"database_url"`
	Port            string                     `yaml:"port" json:"port"`
	AgyBin          string                     `yaml:"agy_bin" json:"agy_bin"`
	APIKey          string                     `yaml:"api_key" json:"api_key"`
	SystemPrompt    string                     `yaml:"system_prompt" json:"system_prompt"`
	DiscordToken    string                     `yaml:"discord_token" json:"discord_token"`
	GitHubPAT       string                     `yaml:"github_pat" json:"github_pat"`
	Ollama          OllamaConfig               `yaml:"ollama" json:"ollama"`
	ClassifierModel string                     `yaml:"classifier_model" json:"classifier_model"`
}

type Config struct {
	current atomic.Pointer[ConfigData]
}

func DefaultConfigData() *ConfigData { ... }
func cloneConfigData(src *ConfigData) *ConfigData { ... }
func NewTestConfig(data *ConfigData) *Config { ... }
func (c *Config) Current() *ConfigData { ... }
func (c *Config) Update(fresh *ConfigData) { ... }
```
2. Export stateless `ResolveChannelPolicy(channels map[string]ChannelPolicy, id, name string) ChannelPolicy` and `IsAdmin(adminUsers []string, ids ...string) bool`.
3. Implement `Reload(activeCfg *Config) error`.
4. In `buildPostgresDSNFromEnv()`: return `""` when `POSTGRES_HOST` is unset (never `postgres:5432`).
5. Unexport `getEnv`. Delete `config.GetEnv`.
6. Update `LoadConfig()` to return `(*Config, error)`.

- [ ] **Step 4: Run tests and race detector to verify it passes**

Run: `go test -race -v ./brain/pkg/config`
Expected: PASS with 0 race warnings.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/config/
git commit -m "feat(config): implement nil-safe atomic snapshot Config with deep-cloning and zero accessors"
```

---

### Task 2: Refactor `brain/pkg/db` to Accept `*config.Config`

**Files:**
- Modify: `brain/pkg/db/db.go`
- Modify: `brain/pkg/db/db_test.go`
- Modify: `brain/pkg/skills/schedule_test.go`

**Interfaces:**
- Consumes: `cfg *config.Config`. Reads `c := cfg.Current(); c.DatabaseURL`.
- Produces: `InitDB(cfg *config.Config) (*sql.DB, error)`. Deletes `GetDBPath()`.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/db/db_test.go`:
```go
func TestInitDB_ConfigPointerInjection(t *testing.T) {
	// 1. Nil config rejected
	if _, err := InitDB(nil); err == nil {
		t.Errorf("expected error when cfg is nil, got nil")
	}

	// 2. Empty connection string rejected
	cfgEmpty := config.NewTestConfig(&config.ConfigData{})
	if _, err := InitDB(cfgEmpty); err == nil {
		t.Errorf("expected error for empty connection string, got nil")
	}

	// 3. Unsupported scheme rejected
	cfgInvalid := config.NewTestConfig(&config.ConfigData{DatabaseURL: "mysql://user:pass@localhost/db"})
	if _, err := InitDB(cfgInvalid); err == nil {
		t.Errorf("expected error for unsupported scheme, got nil")
	}

	// 4. Valid SQLite temp db succeeds (supports both file paths and sqlite:// prefix)
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	cfgValid := config.NewTestConfig(&config.ConfigData{DatabaseURL: "sqlite://" + tmpFile})
	database, err := InitDB(cfgValid)
	if err != nil {
		t.Fatalf("expected valid sqlite initialization, got: %v", err)
	}
	defer database.Close()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run "TestInitDB_ConfigPointerInjection" ./brain/pkg/db`
Expected: FAIL due to signature mismatch.

- [ ] **Step 3: Implement `InitDB(cfg *config.Config)` & excise `GetDBPath()`**

In `brain/pkg/db/db.go`:
1. Change signature to:
```go
func InitDB(cfg *config.Config) (*sql.DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("db: config cannot be nil")
	}
	cur := cfg.Current()
	if cur == nil {
		return nil, fmt.Errorf("db: config snapshot is nil")
	}
	trimmed := strings.TrimSpace(cur.DatabaseURL)
	if trimmed == "" {
		return nil, fmt.Errorf("db: connection string cannot be empty")
	}

	// Normalize sqlite:// prefix to plain path
	trimmed = strings.TrimPrefix(trimmed, "sqlite://")

	isPg := strings.HasPrefix(trimmed, "postgres://") || strings.HasPrefix(trimmed, "postgresql://")
	if !isPg {
		if strings.Contains(trimmed, "://") && !strings.HasPrefix(trimmed, "file://") {
			return nil, fmt.Errorf("db: unsupported database scheme in %q", trimmed)
		}
	}

	if isPg {
		if _, err := stdlib.ParseConfig(trimmed); err != nil {
			return nil, fmt.Errorf("db: invalid postgres connection string: %w", err)
		}
		// postgres connection and advisory lock
	} else {
		// sqlite setup
		if trimmed == ":memory:" || strings.Contains(trimmed, "mode=memory") {
			database.SetMaxOpenConns(1)
		}
	}
	return database, nil
}
```
2. Delete `GetDBPath()` function completely.
3. In `brain/pkg/db/db_test.go`:
   - Replace `setupTestDB` Postgres probing with pure `config.NewTestConfig(&config.ConfigData{DatabaseURL: filepath.Join(t.TempDir(), "test.db")})`.
   - Remove `os.Getenv("TEST_DATABASE_URL")`.
   - Delete `TestGetDBPath_Precedence` and `TestDBPath`.
4. In `brain/pkg/skills/schedule_test.go`:
   - Update `db.InitDB(":memory:")` calls on lines 13 and 73 to pass `config.NewTestConfig(&config.ConfigData{DatabaseURL: ":memory:"})`.

- [ ] **Step 4: Run tests to verify it passes**

Run: `go test -v ./brain/pkg/db ./brain/pkg/skills`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/db/ brain/pkg/skills/schedule_test.go
git commit -m "refactor(db): require *config.Config in InitDB and delete GetDBPath"
```

---

### Task 3: Refactor `brain/pkg/memory` to Accept `*config.Config`

**Files:**
- Modify: `brain/pkg/memory/ollama.go`
- Modify: `brain/pkg/memory/memory_test.go`

**Interfaces:**
- Consumes: `cfg *config.Config`. Reads `cur := client.cfg.Current(); cur.Ollama` JIT inside methods.
- Produces: `NewClient(cfg *config.Config) *Client`. Client holds `cfg *config.Config`.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/memory/memory_test.go`:
```go
func TestNewClient_ConfigPointerInjection(t *testing.T) {
	cfg := config.NewTestConfig(&config.ConfigData{
		Ollama: config.OllamaConfig{
			BaseURL:     "http://localhost:11434",
			Model:       "nomic-embed-text",
			QueryPrefix: "search_query: ",
		},
	})
	client := NewClient(cfg)
	if client == nil {
		t.Fatalf("expected non-nil client")
	}
	if client.cfg != cfg {
		t.Errorf("expected client to store injected *config.Config")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run "TestNewClient_ConfigPointerInjection" ./brain/pkg/memory`
Expected: FAIL due to signature mismatch.

- [ ] **Step 3: Implement `NewClient(cfg *config.Config)`**

In `brain/pkg/memory/ollama.go`:
1. Update `Client` struct:
```go
type Client struct {
	cfg        *config.Config
	httpClient *http.Client
}

func NewClient(cfg *config.Config) *Client {
	return &Client{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}
```
2. In `EmbedQuery` and `EmbedDocuments`, read settings from `c.cfg.Current().Ollama` JIT.
3. Delete all calls to `os.Getenv` in `ollama.go` (lines 30, 64, 69, 71).
4. In `memory_test.go`:
   - Replace `setupTestDB` Postgres probing with pure `config.NewTestConfig(&config.ConfigData{DatabaseURL: filepath.Join(t.TempDir(), "memory_test.db")})`.
   - Remove `os.Getenv("TEST_DATABASE_URL")`.

- [ ] **Step 4: Run tests to verify it passes**

Run: `go test -v ./brain/pkg/memory`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/memory/
git commit -m "refactor(memory): require *config.Config in NewClient and remove os.Getenv"
```

---

### Task 4: Refactor `brain/pkg/sanitizer` for `*config.Config` Registration

**Files:**
- Modify: `brain/pkg/sanitizer/sanitizer.go`
- Modify: `brain/pkg/sanitizer/sanitizer_test.go`

**Interfaces:**
- Consumes: `cfg *config.Config`. Reads `c := cfg.Current(); c.APIKey, c.DiscordToken, c.GitHubPAT, c.DatabaseURL`.
- Produces: `RegisterConfigTokens(cfg *config.Config)` and `ResetSensitiveTokens()`. Removes automatic `os.Environ()` scan.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/sanitizer/sanitizer_test.go`:
```go
func TestSanitizer_RegisterConfigTokens(t *testing.T) {
	ResetSensitiveTokens()
	cfg := config.NewTestConfig(&config.ConfigData{
		APIKey:       "gemini_secret_api_key_12345",
		DiscordToken: "discord_token_secret_abcdef",
		GitHubPAT:    "ghp_testpersonalaccesstoken123456",
		DatabaseURL:  "postgres://aerial:supersecretpass@localhost:5432/aerial",
	})
	RegisterConfigTokens(cfg)

	input := "Error calling Gemini with key gemini_secret_api_key_12345 and db pass supersecretpass"
	sanitized := Sanitize(input)
	if strings.Contains(sanitized, "gemini_secret_api_key_12345") {
		t.Errorf("APIKey was not sanitized: %s", sanitized)
	}
	if strings.Contains(sanitized, "supersecretpass") {
		t.Errorf("Database password was not sanitized: %s", sanitized)
	}

	// Repeated registration does not duplicate tokens
	RegisterConfigTokens(cfg)
	if count := countCachedTokens("gemini_secret_api_key_12345"); count != 1 {
		t.Errorf("expected 1 cached token, got %d", count)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run "TestSanitizer_RegisterConfigTokens" ./brain/pkg/sanitizer`
Expected: FAIL.

- [ ] **Step 3: Implement deduplicating `RegisterConfigTokens` & `ResetSensitiveTokens`**

In `brain/pkg/sanitizer/sanitizer.go`:
1. Implement `RegisterConfigTokens(cfg *config.Config)` with deduplication and URL password extraction:
```go
func RegisterConfigTokens(cfg *config.Config) {
	if cfg == nil || cfg.Current() == nil {
		return
	}
	cur := cfg.Current()
	envMu.Lock()
	defer envMu.Unlock()

	existing := make(map[string]bool, len(envSecretsCache))
	for _, tok := range envSecretsCache {
		existing[tok] = true
	}

	tokens := []string{cur.APIKey, cur.DiscordToken, cur.GitHubPAT}
	if u, err := url.Parse(cur.DatabaseURL); err == nil && u.User != nil {
		if p, ok := u.User.Password(); ok {
			tokens = append(tokens, p)
		}
	}

	for _, tok := range tokens {
		trimmed := strings.TrimSpace(tok)
		if len(trimmed) >= 4 && !existing[trimmed] {
			envSecretsCache = append(envSecretsCache, trimmed)
			existing[trimmed] = true
		}
	}
	sort.Slice(envSecretsCache, func(i, j int) bool {
		return len(envSecretsCache[i]) > len(envSecretsCache[j])
	})
}

func ResetSensitiveTokens() {
	envMu.Lock()
	defer envMu.Unlock()
	envSecretsCache = nil
}
```
2. Delete `envOnce` and `buildEnvSecrets()`. Remove ambient `os.Environ()` and `os.Getenv` scanning.

- [ ] **Step 4: Run tests to verify it passes**

Run: `go test -v ./brain/pkg/sanitizer`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/sanitizer/
git commit -m "refactor(sanitizer): deduplicate RegisterConfigTokens and remove ambient env scanning"
```

---

### Task 5: Refactor `brain/pkg/gitsync` to Accept `*config.Config`

**Files:**
- Modify: `brain/pkg/gitsync/gitsync.go`
- Modify: `brain/pkg/gitsync/gitsync_test.go`

**Interfaces:**
- Consumes: `cfg *config.Config`. Reads `cur := cfg.Current(); cur.GitHubPAT`.
- Produces: `SyncRepo(ctx context.Context, repoPath string, cfg *config.Config) (bool, error)`.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/gitsync/gitsync_test.go`:
```go
func TestSyncRepo_ConfigPointerInjection(t *testing.T) {
	cfg := config.NewTestConfig(&config.ConfigData{
		GitHubPAT: "ghp_mock_token_for_test",
	})
	ctx := context.Background()
	_, _ = SyncRepo(ctx, t.TempDir(), cfg)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run "TestSyncRepo_ConfigPointerInjection" ./brain/pkg/gitsync`
Expected: FAIL due to signature mismatch.

- [ ] **Step 3: Implement `SyncRepo` with `*config.Config`**

In `brain/pkg/gitsync/gitsync.go`:
1. Update signature to:
```go
func SyncRepo(ctx context.Context, repoPath string, cfg *config.Config) (bool, error) {
	var pat string
	if cfg != nil && cfg.Current() != nil {
		pat = cfg.Current().GitHubPAT
	}
	...
}
```
2. Delete `os.Getenv("GITHUB_PAT")`.
3. Update tests in `gitsync_test.go` to pass `config.NewTestConfig(&config.ConfigData{ GitHubPAT: ... })`.

- [ ] **Step 4: Run tests to verify it passes**

Run: `go test -v ./brain/pkg/gitsync`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/gitsync/
git commit -m "refactor(gitsync): require *config.Config in SyncRepo and remove os.Getenv"
```

---

### Task 6: Refactor `brain/pkg/queue` to Consume `*config.Config`

**Files:**
- Modify: `brain/pkg/queue/queue.go`
- Modify: `brain/pkg/queue/queue_test.go`

**Interfaces:**
- Consumes: `appCfg *config.Config`.
- Produces: `WorkerPool` with `p.appCfg *config.Config`. Re-evaluates `p.appCfg.Current()` JIT at burst entry and retry attempts. Deletes `UpdateRuntimeConfig`.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/queue/queue_test.go`:
```go
func TestWorkerPool_DynamicConfigObservation(t *testing.T) {
	appCfg := config.NewTestConfig(&config.ConfigData{
		Model:         "initial-model",
		SystemChannel: "initial-alerts",
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "threads"},
		},
	})
	pool := NewWorkerPool(appCfg, WorkerPoolConfig{})
	if pool.appCfg.Current().Model != "initial-model" {
		t.Errorf("expected initial model, got %q", pool.appCfg.Current().Model)
	}

	// Mutate config atomically
	appCfg.Update(&config.ConfigData{
		Model: "hot-reloaded-model",
	})

	// WorkerPool immediately observes updated model without manual setter
	if pool.appCfg.Current().Model != "hot-reloaded-model" {
		t.Errorf("expected hot-reloaded model, got %q", pool.appCfg.Current().Model)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run "TestWorkerPool_DynamicConfigObservation" ./brain/pkg/queue`
Expected: FAIL.

- [ ] **Step 3: Refactor `WorkerPool` to store `p.appCfg *config.Config`**

In `brain/pkg/queue/queue.go`:
1. Update `WorkerPool` struct and constructor:
```go
type WorkerPool struct {
	appCfg    *config.Config
	cfg       WorkerPoolConfig
	threadChs map[string]*threadWorkerState
	...
}

func NewWorkerPool(appCfg *config.Config, cfg WorkerPoolConfig) *WorkerPool {
	return &WorkerPool{
		appCfg:    appCfg,
		cfg:       cfg,
		threadChs: make(map[string]*threadWorkerState),
		...
	}
}
```
2. In `WorkerPoolConfig`: remove `Model`, `AgyBin`, `APIKey`, and `SystemPrompt`.
3. In `processBurst`:
   - At burst entry and at the top of each retry attempt: `cur := p.appCfg.Current()`.
   - Use `cur.Model`, `cur.AgyBin`, `cur.APIKey`, `cur.SystemPrompt`.
   - Resolve channel policy via `policy := config.ResolveChannelPolicy(cur.Channels, effectiveID, effectiveName)`.
   - Resolve system alerts channel via `cur.SystemChannel`.
4. Delete `WorkerPool.UpdateRuntimeConfig(...)`.
5. Update `queue_test.go` to instantiate `NewWorkerPool(appCfg, WorkerPoolConfig{ DB: db, ... })`.

- [ ] **Step 4: Run tests to verify it passes**

Run: `go test -v ./brain/pkg/queue`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/queue/
git commit -m "refactor(queue): inject *config.Config into WorkerPool and read config via cfg.Current()"
```

---

### Task 7: Refactor `brain/pkg/classifier` and `brain/pkg/scheduler`

**Files:**
- Modify: `brain/pkg/classifier/classifier.go`
- Modify: `brain/pkg/classifier/classifier_test.go`
- Modify: `brain/pkg/scheduler/scheduler.go`
- Modify: `brain/pkg/scheduler/scheduler_test.go`

**Interfaces:**
- Consumes: `cfg *config.Config`.
- Produces: `classifier.NewClassifier(cfg *config.Config, runnerFn runner.RunnerFunc)`, `scheduler.Scheduler` struct with `NewScheduler(cfg *config.Config, ...)` and `Start(...)`.

- [ ] **Step 1: Write failing tests**

In `brain/pkg/classifier/classifier_test.go`:
```go
func TestClassifier_ConfigPointerInjection(t *testing.T) {
	cfg := config.NewTestConfig(&config.ConfigData{
		ClassifierModel: "test-classifier-model",
	})
	cls := NewClassifier(cfg, nil)
	if cls.cfg != cfg {
		t.Errorf("expected classifier to store *config.Config")
	}
}
```

In `brain/pkg/scheduler/scheduler_test.go`:
```go
func TestScheduler_ConfigPointerInjection(t *testing.T) {
	cfg := config.NewTestConfig(&config.ConfigData{
		Timezone: "America/Chicago",
		Model:    "scheduler-model",
	})
	sched := NewScheduler(cfg, nil, nil, nil, nil)
	if sched.cfg != cfg {
		t.Errorf("expected scheduler to store *config.Config")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -v ./brain/pkg/classifier ./brain/pkg/scheduler`
Expected: FAIL.

- [ ] **Step 3: Implement `Classifier` and `Scheduler` with `*config.Config`**

1. In `brain/pkg/classifier/classifier.go`:
   - Refactor `Classifier` to hold `cfg *config.Config`.
   - In `Classify()`: evaluate `cur := c.cfg.Current(); cur.ClassifierModel, cur.AgyBin, cur.APIKey` JIT.
2. In `brain/pkg/scheduler/scheduler.go`:
   - Define `Scheduler` struct:
     ```go
     type Scheduler struct {
         cfg           *config.Config
         db            *sql.DB
         pool          *queue.WorkerPool
         dgSession     *discordgo.Session
         threadCreator ThreadCreator
         memClient     *memory.Client
     }
     func NewScheduler(cfg *config.Config, db *sql.DB, pool *queue.WorkerPool, dgSession *discordgo.Session, memClient *memory.Client) *Scheduler
     ```
   - In `s.ProcessDueSchedules`: read `cur := s.cfg.Current(); cur.Timezone, cur.Channels`.
   - In `s.ExtractFactsLLM`: read `cur := s.cfg.Current(); cur.AgyBin, cur.APIKey, cur.Model`.
   - Eliminate all calls to `config.GetEnv`, `config.GetTimezone()`, and `config.GetRuntimeConfig()`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -v ./brain/pkg/classifier ./brain/pkg/scheduler`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/classifier/ brain/pkg/scheduler/
git commit -m "refactor(classifier,scheduler): inject *config.Config and read fields JIT via cfg.Current()"
```

---

### Task 8: Refactor `scheduler-mcp` for Pure Constructor Dependency Injection

**Files:**
- Create: `scheduler-mcp/config.go`
- Modify: `scheduler-mcp/db.go`
- Modify: `scheduler-mcp/tools.go`
- Modify: `scheduler-mcp/main.go`
- Modify: `scheduler-mcp/tools_test.go`
- Modify: `scheduler-mcp/server_test.go`

**Interfaces:**
- Consumes: `*Config` struct.
- Produces: `InitDB(cfg *Config) (*sql.DB, error)`, `NewToolHandler(cfg *Config, database *sql.DB) *ToolHandler`. Deletes `GetDBPath()` and all `os.Getenv`.

- [ ] **Step 1: Write the failing test**

In `scheduler-mcp/tools_test.go`:
```go
func TestInitDB_ConfigPointer(t *testing.T) {
	cfg := &Config{DatabaseURL: ":memory:", Timezone: "America/Chicago"}
	database, err := InitDB(cfg)
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	defer database.Close()

	handler := NewToolHandler(cfg, database)
	if handler.cfg != cfg {
		t.Errorf("expected handler to retain *Config")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run "TestInitDB_ConfigPointer" ./scheduler-mcp`
Expected: FAIL.

- [ ] **Step 3: Implement `scheduler-mcp/config.go` & refactor `db.go`, `tools.go`, `server_test.go`**

1. Create `scheduler-mcp/config.go`:
```go
package main

type Config struct {
	DatabaseURL string
	Timezone    string
	Port        string
}

func LoadConfig() (*Config, error) { ... }
```
2. In `scheduler-mcp/db.go`:
   - Refactor `InitDB(cfg *Config) (*sql.DB, error)`.
   - Set `database.SetMaxOpenConns(1)` for in-memory SQLite.
   - Use `conn, err := database.Conn(ctx)` to acquire `SELECT pg_advisory_lock(849201948201)` during PostgreSQL migrations.
   - Align schema DDL with `brain/pkg/db/schema.go`.
   - Delete `GetDBPath()` and all calls to `os.Getenv`.
3. In `scheduler-mcp/tools.go`:
   - Refactor `NewToolHandler(cfg *Config, database *sql.DB) *ToolHandler`.
   - Handler reads `h.cfg.Timezone`.
4. In `scheduler-mcp/server_test.go` and `tools_test.go`:
   - Update `setupTestServer` to pass `&Config{DatabaseURL: ":memory:"}`.
   - Delete `TestPostgresSchedules` and all `TEST_DATABASE_URL` / `GetDBPath` test assertions.

- [ ] **Step 4: Run tests to verify it passes**

Run: `go test -v ./scheduler-mcp`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scheduler-mcp/
git commit -m "refactor(scheduler-mcp): pure *Config pointer constructor injection and advisory locks"
```

---

### Task 9: Wire Pure Composition Root & Serialized Hot-Reload in `brain/main.go` & `funnel.go`

**Files:**
- Modify: `brain/main.go`
- Modify: `brain/funnel.go`
- Modify: `brain/main_test.go`

**Interfaces:**
- Consumes: `*config.Config` from `config.LoadConfig()`.
- Produces: Pure composition root passing `cfg *config.Config` to all subpackages. Serialized hot reload with `reloadMu` and early error exit.

- [ ] **Step 1: Write verification test for composition root**

In `brain/main_test.go`: verify `RunBrainApp` starts and stops cleanly with a pure test `*config.Config`:
```go
func TestRunBrainApp_PureConfig(t *testing.T) {
	cfg := config.NewTestConfig(&config.ConfigData{
		DatabaseURL:  filepath.Join(t.TempDir(), "brain_test.db"),
		Port:         "0",
		Model:        "test-model",
		Timezone:     "UTC",
		SystemPrompt: "test prompt",
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "threads"},
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Immediate cancellation to test lifecycle shutdown

	err := RunBrainApp(ctx, cfg)
	if err != nil && err != http.ErrServerClosed {
		t.Errorf("expected clean shutdown, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run "TestRunBrainApp_PureConfig" ./brain`
Expected: FAIL due to signature mismatch.

- [ ] **Step 3: Implement composition root in `main.go` and `funnel.go`**

1. In `funnel.go`:
   - Refactor `connectDiscordFunnel(ctx context.Context, database *sql.DB, pool *queue.WorkerPool, cfg *config.Config) *discordgo.Session`.
   - Read `c := cfg.Current()`, use `config.ResolveChannelPolicy(c.Channels, ...)`, `c.DiscordToken`.
   - Remove `config.GetRuntimeConfig()`.
2. In `main.go`:
   - `RunBrainApp(ctx context.Context, cfg *config.Config) error`.
   - Initialize `db.InitDB(cfg)`, `memory.NewClient(cfg)`, `sanitizer.RegisterConfigTokens(cfg)`, `classifier.NewClassifier(cfg, runner.RunAgy)`.
   - Create `WorkerPool(cfg, WorkerPoolConfig{ DB: database, Classifier: cls, ... })`.
   - Start `scheduler.NewScheduler(cfg, database, pool, dgSession, memClient)`.
   - Guarded hot reload function:
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
             return // Early return protects on-disk files from corrupted config
         }

         sanitizer.RegisterConfigTokens(cfg)
         cur := cfg.Current()
         _ = config.EnsureAgySettings(cur.APIKey, cur.Model)
         _ = config.EnsureMcpConfig(cur.McpServers)
         _ = config.EnsureSystemRules(cur.SystemPrompt)
         _ = skills.EnsureSkills()
     }
     ```
   - Delete `NewBrainConfigFromEnv` and `BrainConfig` wrapper struct.
   - In `main()`: `cfg, err := config.LoadConfig()`, then `RunBrainApp(ctx, cfg)`.

- [ ] **Step 4: Run tests to verify it passes**

Run: `go test -v ./brain`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/main.go brain/funnel.go brain/main_test.go
git commit -m "refactor(main): pure composition root with atomic *config.Config injection and serialized hot-reload"
```

---

### Task 10: Clean-Room Test Harnesses & Monorepo Verification

**Files:**
- Modify: `scripts/verify.sh`
- Modify: `scripts/check-coverage.sh`
- Modify: `scripts/migrate_sqlite_to_postgres.go`
- Modify: `.github/workflows/docker-publish.yml`

**Interfaces:**
- Consumes: Test environment.
- Produces: OS-aware clean-room allowlist execution. Complete excision of `TEST_DATABASE_URL` across monorepo.

- [ ] **Step 1: Write clean-room execution in `scripts/verify.sh` and `scripts/check-coverage.sh`**

Implement `run_clean_test` using POSIX parameter expansion `${VAR:+VAR="$VAR"}`:
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
In `scripts/check-coverage.sh`: remove `|| true` and `>/dev/null 2>&1`, recording any test exit failure as a blocking violation.

- [ ] **Step 2: Remove `TEST_DATABASE_URL` from `scripts/migrate_sqlite_to_postgres.go` & `.github/workflows/docker-publish.yml`**

- In `scripts/migrate_sqlite_to_postgres.go:81`: delete `targetDSN = os.Getenv("TEST_DATABASE_URL")`.
- In `.github/workflows/docker-publish.yml`: remove `TEST_DATABASE_URL: postgres://...`.

- [ ] **Step 3: Run static analysis invariant checks**

Execute the 4 non-negotiable grep checks:
```bash
test $(git grep -E "os\.(Getenv|LookupEnv|Environ|ExpandEnv)" brain/pkg/ | grep -v "brain/pkg/config" | wc -l) -eq 0
test $(git grep "config.GetEnv" brain/ | wc -l) -eq 0
test $(git grep -I "TEST_DATABASE_URL" -- ':(exclude)docs/' ':(exclude)*.md' | wc -l) -eq 0
test $(git grep -I "GetDBPath" -- ':(exclude)docs/' ':(exclude)*.md' | wc -l) -eq 0
```

- [ ] **Step 4: Run clean-room verification under dirty environment**

Run:
```bash
POSTGRES_HOST=postgres POSTGRES_PASSWORD=badpass GITHUB_PAT=fake sh scripts/verify.sh --full
```
Expected: PASS 100%. Zero connections to live PostgreSQL. Zero poison pill alerts.

- [ ] **Step 5: Run coverage check**

Run:
```bash
sh scripts/check-coverage.sh --check
```
Expected: All package floors and total coverage checks PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/ .github/
git commit -m "ci: enforce cross-platform clean-room test execution and invariant verification"
```
