# Hermetic Configuration & Pure Pointer Dependency Injection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish hermetic configuration parsing and pure `*config.Config` pointer constructor dependency injection across the Aerial monorepo, permanently eliminating ambient environment leakage, fragile `env -u` denylists, backdoor getters, and toxic postgres fallback traps.

**Architecture:** Consolidate all environment variable and config-file parsing into `brain/pkg/config`. Every package other than `config` (`pkg/db`, `pkg/memory`, `pkg/queue`, `pkg/runner`, `pkg/scheduler`, `pkg/sanitizer`, `pkg/gitsync`, `pkg/session`, `pkg/skills`, `pkg/watcher`) must receive a pointer to `*config.Config` in its constructor, and that pointer is their **sole mechanism** for accessing configuration and their **only interaction** with `pkg/config`. No subpackage may ever call `os.Getenv`, `config.GetEnv`, `LoadDefaultConfig`, or package-level getters. Hot reload is handled atomically inside `config.Config` via internal read-write locking (`Update()` / `Reload()`), so all components holding `*config.Config` observe live updates safely and immediately without manual cross-package setters. Tests instantiate `&config.Config{ DatabaseURL: ":memory:", ... }` directly with zero environment variable manipulation.

**Tech Stack:** Go 1.24, modernc.org/sqlite, jackc/pgx/v5, gopkg.in/yaml.v3, POSIX sh test harness.

**Spec:** [`docs/superpowers/specs/2026-09-06-hermetic-config-dependency-injection-design.md`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/docs/superpowers/specs/2026-09-06-hermetic-config-dependency-injection-design.md)

## Global Constraints

- Universal constructor injection: Every package other than `config` must receive `cfg *config.Config` in its constructor.
- Single access path: Reading from `cfg *config.Config` is the ONLY interaction subpackages have with `config`.
- Zero `os.Getenv` or `os.LookupEnv` in `brain/pkg/db`, `brain/pkg/memory`, `brain/pkg/gitsync`, `brain/pkg/sanitizer`, `brain/pkg/queue`, `brain/pkg/runner`, `brain/pkg/scheduler`, `brain/pkg/session`, or `brain/pkg/watcher`.
- `config.GetEnv` is deleted and unexported (`getEnv` private to `brain/pkg/config`).
- `config.Config` manages thread-safe atomic hot-reloads via `mu sync.RWMutex`, `Update(newCfg)`, and `Reload(activeCfg)`.
- `TEST_DATABASE_URL` is completely excised from all code, tests, scripts, and workflows.
- `DefaultConfig()` and `buildPostgresDSNFromEnv()` must never default to hostname `postgres:5432` when `POSTGRES_HOST` is unset.
- Unit tests must never call `os.Getenv` or `t.Setenv` for infrastructure/credentials. (Only `config_test.go` and `dashboard/main_test.go` retain scoped `t.Setenv` to test config file/env fallback parsing).
- Constructors strictly validate required parameters: `db.InitDB(cfg)` returns error if `cfg == nil` or connection string is empty. Unrecognized URL schemes return errors instead of silently falling through to SQLite.
- All Go test suites must compile and pass under clean-room execution at every commit.
- Monorepo Go statement coverage must satisfy all package floors and threshold checks.

---

### Task 1: Refactor `brain/pkg/config` for Universal `*Config` Injection & Atomic Hot Reload

**Files:**
- Modify: `brain/pkg/config/config.go`
- Test: `brain/pkg/config/config_test.go`

**Interfaces:**
- Consumes: Environment variables (`DATABASE_URL`, `POSTGRES_*`, `PORT`, `AGY_BIN`, `GEMINI_API_KEY`, `ANTIGRAVITY_API_KEY`, `SYSTEM_PROMPT`, `DISCORD_TOKEN`, `DISCORD_BOT_TOKEN`, `GITHUB_PAT`, `OLLAMA_URL`, `EMBEDDING_MODEL`, `OLLAMA_EMBEDDING_MODEL`, `EMBEDDING_QUERY_PREFIX`, `CLASSIFIER_MODEL`, `AMBIENT_CLASSIFIER_MODEL`).
- Produces: `*config.Config` with embedded `sync.RWMutex`, thread-safe accessors (`GetModel()`, `GetTimezone()`, `GetSystemChannel()`, `GetDatabaseURL()`, `GetPort()`, `GetAgyBin()`, `GetAPIKey()`, `GetSystemPrompt()`, `GetDiscordToken()`, `GetGitHubPAT()`, `GetClassifierModel()`, `GetOllama()`, `GetGitSync()`, `GetMcpServers()`, `ResolveChannelPolicy()`, `IsAdmin()`), `Update(newCfg *Config)`, `Reload(activeCfg *Config)`. Unexports `getEnv` and deletes `config.GetEnv`.

- [ ] **Step 1: Write the failing test**

Add concurrency and atomic update tests to `brain/pkg/config/config_test.go`:
```go
func TestConfig_AtomicUpdateAndAccessors(t *testing.T) {
	cfg := &Config{
		Model:         "initial-model",
		Timezone:      "America/Los_Angeles",
		SystemChannel: "initial-alerts",
		DatabaseURL:   ":memory:",
		Port:          "8080",
		AgyBin:        "agy",
		APIKey:        "initial-key",
		SystemPrompt:  "initial-prompt",
		DiscordToken:  "initial-token",
		GitHubPAT:     "initial-pat",
		ClassifierModel: "Gemini 3.8 Flash (Low)",
		Ollama: OllamaConfig{
			BaseURL:     "http://localhost:11434",
			Model:       "nomic-embed-text",
			QueryPrefix: "search_query: ",
		},
	}

	if cfg.GetModel() != "initial-model" {
		t.Errorf("expected GetModel 'initial-model', got %q", cfg.GetModel())
	}
	if cfg.GetTimezone() != "America/Los_Angeles" {
		t.Errorf("expected GetTimezone 'America/Los_Angeles', got %q", cfg.GetTimezone())
	}
	if cfg.GetDatabaseURL() != ":memory:" {
		t.Errorf("expected GetDatabaseURL ':memory:', got %q", cfg.GetDatabaseURL())
	}

	// Test concurrent atomic update
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = cfg.GetModel()
			_ = cfg.GetTimezone()
			_ = cfg.GetSystemChannel()
			_ = cfg.GetDatabaseURL()
			_ = cfg.GetOllama()
		}(i)
	}

	updateCfg := &Config{
		Model:         "updated-model",
		Timezone:      "America/Chicago",
		SystemChannel: "updated-alerts",
		DatabaseURL:   "sqlite://test.db",
	}
	cfg.Update(updateCfg)
	wg.Wait()

	if cfg.GetModel() != "updated-model" {
		t.Errorf("expected updated model, got %q", cfg.GetModel())
	}
	if cfg.GetTimezone() != "America/Chicago" {
		t.Errorf("expected updated timezone, got %q", cfg.GetTimezone())
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

Run: `go test -v -run "TestConfig_AtomicUpdateAndAccessors|TestConfig_PostgresHostUnset_ReturnsEmptyDSN" ./brain/pkg/config`
Expected: FAIL due to missing fields/accessors.

- [ ] **Step 3: Implement atomic accessors, `Update()`, `Reload()`, and safe defaults**

In `brain/pkg/config/config.go`:
1. Embed `mu sync.RWMutex` in `Config` struct.
2. Add infrastructure fields: `DatabaseURL`, `Port`, `AgyBin`, `APIKey`, `SystemPrompt`, `DiscordToken`, `GitHubPAT`, `Ollama OllamaConfig`, `ClassifierModel`.
3. Add accessors: `GetModel()`, `GetTimezone()`, `GetSystemChannel()`, `GetDatabaseURL()`, `GetPort()`, `GetAgyBin()`, `GetAPIKey()`, `GetSystemPrompt()`, `GetDiscordToken()`, `GetGitHubPAT()`, `GetClassifierModel()`, `GetOllama()`, `GetGitSync()`, `GetMcpServers()`.
4. Implement `cfg.Update(newCfg *Config)` and `Reload(activeCfg *Config) error`.
5. Update `buildPostgresDSNFromEnv()`: if `POSTGRES_HOST` is unset and `DATABASE_URL` is unset, return `""` (never `postgres:5432`).
6. Unexport `getEnv(key, defaultVal string) string`. Delete `config.GetEnv`.
7. In `LoadConfigFromPaths`: return `(*Config, error)`.

- [ ] **Step 4: Run tests and race detector to verify it passes**

Run: `go test -race -v ./brain/pkg/config`
Expected: PASS with 0 race warnings.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/config/
git commit -m "feat(config): implement thread-safe *Config, atomic hot-reload, and safe postgres defaults"
```

---

### Task 2: Refactor `brain/pkg/db` to Accept `*config.Config`

**Files:**
- Modify: `brain/pkg/db/db.go`
- Modify: `brain/pkg/db/db_test.go`
- Modify: `brain/pkg/db/schedules_test.go`

**Interfaces:**
- Consumes: `cfg *config.Config`.
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
	cfgEmpty := &config.Config{}
	if _, err := InitDB(cfgEmpty); err == nil {
		t.Errorf("expected error for empty connection string, got nil")
	}

	// 3. Unsupported scheme rejected
	cfgInvalid := &config.Config{DatabaseURL: "mysql://user:pass@localhost/db"}
	if _, err := InitDB(cfgInvalid); err == nil {
		t.Errorf("expected error for unsupported scheme, got nil")
	}

	// 4. Valid SQLite temp db succeeds
	tmpFile := filepath.Join(t.TempDir(), "test.db")
	cfgValid := &config.Config{DatabaseURL: tmpFile}
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
	trimmed := strings.TrimSpace(cfg.GetDatabaseURL())
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
		if _, err := stdlib.ParseConfig(trimmed); err != nil {
			return nil, fmt.Errorf("db: invalid postgres connection string: %w", err)
		}
		// postgres pool setup & pg_advisory_lock
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
3. In `db_test.go` and `schedules_test.go`:
   - Replace all `InitDB(path)` calls with `InitDB(&config.Config{DatabaseURL: path})`.
   - Remove `os.Getenv("TEST_DATABASE_URL")`.
   - Delete `TestGetDBPath_Precedence` and `TestDBPath`.

- [ ] **Step 4: Run tests to verify it passes**

Run: `go test -v ./brain/pkg/db`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/db/
git commit -m "refactor(db): require *config.Config in InitDB and delete GetDBPath"
```

---

### Task 3: Refactor `brain/pkg/memory` to Accept `*config.Config`

**Files:**
- Modify: `brain/pkg/memory/ollama.go`
- Modify: `brain/pkg/memory/memory_test.go`

**Interfaces:**
- Consumes: `cfg *config.Config`.
- Produces: `NewClient(cfg *config.Config) *Client`. Client holds `cfg *config.Config`.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/memory/memory_test.go`:
```go
func TestNewClient_ConfigPointerInjection(t *testing.T) {
	cfg := &config.Config{
		Ollama: config.OllamaConfig{
			BaseURL:     "http://localhost:11434",
			Model:       "nomic-embed-text",
			QueryPrefix: "search_query: ",
		},
	}
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
2. In embedding functions, read settings from `c.cfg.GetOllama()`:
   - `baseURL := c.cfg.GetOllama().BaseURL`
   - `model := c.cfg.GetOllama().Model`
   - `prefix := c.cfg.GetOllama().QueryPrefix`
3. Delete all calls to `os.Getenv` in `ollama.go` (lines 30, 64, 69, 71).
4. In `memory_test.go`:
   - Update tests to construct `&config.Config{ Ollama: config.OllamaConfig{ BaseURL: s.URL, ... } }`.
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
- Consumes: `cfg *config.Config`.
- Produces: `RegisterConfigTokens(cfg *config.Config)` and `ResetSensitiveTokens()`. Removes automatic `os.Environ()` scan.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/sanitizer/sanitizer_test.go`:
```go
func TestSanitizer_RegisterConfigTokens(t *testing.T) {
	ResetSensitiveTokens()
	cfg := &config.Config{
		APIKey:       "gemini_secret_api_key_12345",
		DiscordToken: "discord_token_secret_abcdef",
		GitHubPAT:    "ghp_testpersonalaccesstoken123456",
	}
	RegisterConfigTokens(cfg)

	input := "Error calling Gemini with key gemini_secret_api_key_12345 and token discord_token_secret_abcdef"
	sanitized := Sanitize(input)
	if strings.Contains(sanitized, "gemini_secret_api_key_12345") {
		t.Errorf("APIKey was not sanitized: %s", sanitized)
	}
	if strings.Contains(sanitized, "discord_token_secret_abcdef") {
		t.Errorf("DiscordToken was not sanitized: %s", sanitized)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run "TestSanitizer_RegisterConfigTokens" ./brain/pkg/sanitizer`
Expected: FAIL due to undefined `RegisterConfigTokens`.

- [ ] **Step 3: Implement `RegisterConfigTokens` & `ResetSensitiveTokens`**

In `brain/pkg/sanitizer/sanitizer.go`:
1. Implement `RegisterConfigTokens(cfg *config.Config)`:
```go
func RegisterConfigTokens(cfg *config.Config) {
	if cfg == nil {
		return
	}
	envMu.Lock()
	defer envMu.Unlock()

	tokens := []string{
		cfg.GetAPIKey(),
		cfg.GetDiscordToken(),
		cfg.GetGitHubPAT(),
	}
	for _, tok := range tokens {
		trimmed := strings.TrimSpace(tok)
		if len(trimmed) >= 4 {
			envSecretsCache = append(envSecretsCache, trimmed)
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
2. Remove ambient `os.Environ()` scanning from default init.

- [ ] **Step 4: Run tests to verify it passes**

Run: `go test -v ./brain/pkg/sanitizer`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/sanitizer/
git commit -m "refactor(sanitizer): add RegisterConfigTokens(*config.Config) and remove ambient env scan"
```

---

### Task 5: Refactor `brain/pkg/gitsync` to Accept `*config.Config`

**Files:**
- Modify: `brain/pkg/gitsync/gitsync.go`
- Modify: `brain/pkg/gitsync/gitsync_test.go`

**Interfaces:**
- Consumes: `cfg *config.Config`.
- Produces: `SyncRepo(ctx context.Context, repoPath string, cfg *config.Config) (bool, error)`.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/gitsync/gitsync_test.go`:
```go
func TestSyncRepo_ConfigPointerInjection(t *testing.T) {
	cfg := &config.Config{
		GitHubPAT: "ghp_mock_token_for_test",
	}
	// Verify signature compiles with *config.Config
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
	if cfg != nil {
		pat = cfg.GetGitHubPAT()
	}
	// use pat in git credentials
	...
}
```
2. Delete `os.Getenv("GITHUB_PAT")`.
3. Update tests in `gitsync_test.go` to pass `&config.Config{ GitHubPAT: ... }`.

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
- Consumes: `cfg *config.Config`.
- Produces: `WorkerPoolConfig{ Config: cfg, DB: db, ... }`. `WorkerPool` reads model, prompts, system channel, and channel policy from `p.cfg`.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/queue/queue_test.go`:
```go
func TestWorkerPool_DynamicConfigObservation(t *testing.T) {
	cfg := &config.Config{
		Model:         "initial-model",
		SystemChannel: "initial-alerts",
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "threads"},
		},
	}
	pool := NewWorkerPool(WorkerPoolConfig{
		Config: cfg,
	})
	if pool.cfg.GetModel() != "initial-model" {
		t.Errorf("expected initial model, got %q", pool.cfg.GetModel())
	}

	// Mutate config atomically
	cfg.Update(&config.Config{
		Model: "hot-reloaded-model",
	})

	// WorkerPool immediately observes updated model without manual setter
	if pool.cfg.GetModel() != "hot-reloaded-model" {
		t.Errorf("expected hot-reloaded model, got %q", pool.cfg.GetModel())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run "TestWorkerPool_DynamicConfigObservation" ./brain/pkg/queue`
Expected: FAIL due to missing `Config` field in `WorkerPoolConfig`.

- [ ] **Step 3: Refactor `WorkerPool` to store and consume `p.cfg *config.Config`**

In `brain/pkg/queue/queue.go`:
1. Add `Config *config.Config` to `WorkerPoolConfig`.
2. In `NewWorkerPool`: store `p.cfg = cfg.Config`.
3. In `processBurst`:
   - Replace direct model access with `p.cfg.GetModel()`.
   - Replace direct AgyBin access with `p.cfg.GetAgyBin()`.
   - Replace direct APIKey access with `p.cfg.GetAPIKey()`.
   - Replace direct SystemPrompt access with `p.cfg.GetSystemPrompt()`.
   - Replace policy resolution with `p.cfg.ResolveChannelPolicy(...)`.
   - Replace system alert channel with `p.cfg.GetSystemChannel()`.
4. In `queue_test.go`:
   - Update tests to construct `WorkerPoolConfig{ Config: &config.Config{ ... }, DB: db, ... }`.
   - Replace `config.GetSystemChannel()` assertions with `cfg.GetSystemChannel()`.

- [ ] **Step 4: Run tests to verify it passes**

Run: `go test -v ./brain/pkg/queue`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/queue/
git commit -m "refactor(queue): inject *config.Config into WorkerPool and read config dynamically"
```

---

### Task 7: Refactor `brain/pkg/scheduler` to Consume `*config.Config`

**Files:**
- Modify: `brain/pkg/scheduler/scheduler.go`
- Modify: `brain/pkg/scheduler/scheduler_test.go`

**Interfaces:**
- Consumes: `cfg *config.Config`.
- Produces: `scheduler.Start(ctx, database, pool, dgSession, memClient, cfg *config.Config) func()`. Eliminates all calls to `config.GetEnv`, `config.GetTimezone()`, and `config.GetRuntimeConfig()`.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/scheduler/scheduler_test.go`:
```go
func TestScheduler_ConfigPointerInjection(t *testing.T) {
	cfg := &config.Config{
		Timezone: "America/Chicago",
		Model:    "scheduler-model",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopFn := Start(ctx, nil, nil, nil, nil, cfg)
	defer stopFn()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run "TestScheduler_ConfigPointerInjection" ./brain/pkg/scheduler`
Expected: FAIL due to signature mismatch.

- [ ] **Step 3: Implement `scheduler.Start` with `*config.Config`**

In `brain/pkg/scheduler/scheduler.go`:
1. Update `Start` signature:
```go
func Start(ctx context.Context, database *sql.DB, pool *queue.WorkerPool, dgSession *discordgo.Session, memClient *memory.Client, cfg *config.Config) func()
```
2. In `scheduler.go`:
   - Line 73: `return s.cfg.GetTimezone()`
   - Line 160: `policy := s.cfg.ResolveChannelPolicy(c.TargetID, channelName)`
   - Lines 312-317:
     ```go
     apiKey := s.cfg.GetAPIKey()
     model := s.cfg.GetModel()
     agyBin := s.cfg.GetAgyBin()
     ```
   - Delete all calls to `config.GetEnv`, `config.GetTimezone()`, `config.GetRuntimeConfig()`.
3. In `scheduler_test.go`:
   - Replace file-based `config.LoadConfigFromPaths(yamlPath)` test mutations with pure `&config.Config{ Timezone: ... }` struct literals passed to `Start()`.

- [ ] **Step 4: Run tests to verify it passes**

Run: `go test -v ./brain/pkg/scheduler`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/scheduler/
git commit -m "refactor(scheduler): require *config.Config in Start and eliminate ambient getters"
```

---

### Task 8: Refactor `scheduler-mcp` for Pure Constructor Dependency Injection

**Files:**
- Create: `scheduler-mcp/config.go`
- Modify: `scheduler-mcp/db.go`
- Modify: `scheduler-mcp/tools.go`
- Modify: `scheduler-mcp/main.go`
- Modify: `scheduler-mcp/tools_test.go`

**Interfaces:**
- Consumes: `*Config` struct.
- Produces: `InitDB(cfg *Config) (*sql.DB, error)`, `NewToolHandler(cfg *Config, database *sql.DB) *ToolHandler`. Deletes `GetDBPath()`.

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

- [ ] **Step 3: Implement `scheduler-mcp/config.go` & refactor `db.go` / `tools.go`**

1. Create `scheduler-mcp/config.go`:
```go
package main

type Config struct {
	DatabaseURL string
	Timezone    string
	Port        string
}

func LoadConfig() (*Config, error) {
	// Reads env strictly within scheduler-mcp
	...
}
```
2. In `scheduler-mcp/db.go`:
   - Refactor `InitDB(cfg *Config) (*sql.DB, error)`.
   - Set `database.SetMaxOpenConns(1)` for in-memory SQLite.
   - Use `SELECT pg_advisory_lock(849201948201)` during PostgreSQL migrations.
   - Delete `GetDBPath()` and all calls to `os.Getenv`.
3. In `scheduler-mcp/tools.go`:
   - Refactor `NewToolHandler(cfg *Config, database *sql.DB) *ToolHandler`.
   - Handler reads `h.cfg.Timezone`.
4. In `tools_test.go`:
   - Delete `TestPostgresSchedules` and all `TEST_DATABASE_URL` references.

- [ ] **Step 4: Run tests to verify it passes**

Run: `go test -v ./scheduler-mcp`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scheduler-mcp/
git commit -m "refactor(scheduler-mcp): pure *Config pointer constructor injection and advisory locks"
```

---

### Task 9: Wire Pure Composition Root & Atomic Hot-Reload in `brain/main.go` & `funnel.go`

**Files:**
- Modify: `brain/main.go`
- Modify: `brain/funnel.go`

**Interfaces:**
- Consumes: `*config.Config` from `config.LoadConfig()`.
- Produces: Pure composition root passing `cfg *config.Config` to all subpackages. Atomic hot reload via `config.Reload(cfg)`.

- [ ] **Step 1: Write verification test for composition root**

In `brain/main_test.go`: verify `RunBrainApp` starts and stops cleanly with a pure test `*config.Config`:
```go
func TestRunBrainApp_PureConfig(t *testing.T) {
	cfg := &config.Config{
		DatabaseURL:  filepath.Join(t.TempDir(), "brain_test.db"),
		Port:         "0",
		Model:        "test-model",
		Timezone:     "UTC",
		SystemPrompt: "test prompt",
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "threads"},
		},
	}
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
   - Read `cfg.ResolveChannelPolicy(...)`, `cfg.IsAdmin(...)`, `cfg.GetDiscordToken()`.
   - Remove `config.GetRuntimeConfig()`.
2. In `main.go`:
   - `RunBrainApp(ctx context.Context, cfg *config.Config) error`.
   - Initialize `db.InitDB(cfg)`, `memory.NewClient(cfg)`, `sanitizer.RegisterConfigTokens(cfg)`.
   - Create `WorkerPool` passing `cfg`.
   - Start `scheduler.Start(..., cfg)`.
   - Hot reload function:
     ```go
     reloadConfig := func(source string) {
         if err := config.Reload(cfg); err != nil {
             log.Printf("[%s] Warning: config reload error: %v (retaining LKGC)", source, err)
             if dgSession != nil {
                 _ = delivery.SendSystemAlert(dgSession, cfg.GetSystemChannel(), "Invalid Configuration File", err.Error())
             }
         }
         sanitizer.RegisterConfigTokens(cfg)
         _ = config.EnsureAgySettings(cfg.GetAPIKey(), cfg.GetModel())
         _ = config.EnsureMcpConfig(cfg.GetGitHubPAT(), cfg.GetMcpServers())
         _ = config.EnsureSystemRules(cfg.GetSystemPrompt())
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
git commit -m "refactor(main): pure composition root with atomic *config.Config injection"
```

---

### Task 10: Clean-Room Test Harnesses & Monorepo Verification

**Files:**
- Modify: `scripts/verify.sh`
- Modify: `scripts/check-coverage.sh`
- Modify: `.github/workflows/docker-publish.yml`

**Interfaces:**
- Consumes: Test environment.
- Produces: OS-aware clean-room allowlist execution. Zero credential leakage.

- [ ] **Step 1: Write clean-room execution in `scripts/verify.sh` and `scripts/check-coverage.sh`**

Implement `run_clean_test` in both scripts:
```bash
run_clean_test() {
    env -i \
        PATH="$PATH" \
        HOME="$HOME" \
        GOROOT="${GOROOT:-}" \
        GOPATH="${GOPATH:-}" \
        GOCACHE="${GOCACHE:-}" \
        TMPDIR="${TMPDIR:-}" \
        SystemRoot="${SystemRoot:-${SYSTEMROOT:-}}" \
        SYSTEMROOT="${SYSTEMROOT:-${SystemRoot:-}}" \
        USERPROFILE="${USERPROFILE:-}" \
        TMP="${TMP:-}" \
        TEMP="${TEMP:-}" \
        LOCALAPPDATA="${LOCALAPPDATA:-}" \
        APPDATA="${APPDATA:-}" \
        COMSPEC="${COMSPEC:-}" \
        PATHEXT="${PATHEXT:-}" \
        LANG="${LANG:-en_US.UTF-8}" \
        LC_ALL="${LC_ALL:-en_US.UTF-8}" \
        GIT_CONFIG_NOSYSTEM=1 \
        GIT_TERMINAL_PROMPT=0 \
        CGO_ENABLED="${CGO_ENABLED:-0}" \
        go test "$@"
}
```

- [ ] **Step 2: Run static analysis invariant checks**

Execute the 4 non-negotiable grep checks:
1. `git grep "os.Getenv" brain/pkg/ | grep -v "brain/pkg/config"` -> MUST BE 0.
2. `git grep "config.GetEnv" brain/` -> MUST BE 0.
3. `git grep "TEST_DATABASE_URL"` -> MUST BE 0.
4. `git grep "GetDBPath"` -> MUST BE 0.

- [ ] **Step 3: Run clean-room verification under dirty environment**

Run:
```bash
POSTGRES_HOST=postgres POSTGRES_PASSWORD=badpass GITHUB_PAT=fake sh scripts/verify.sh --full
```
Expected: PASS 100%. Zero connections to live PostgreSQL. Zero poison pill alerts.

- [ ] **Step 4: Run coverage check**

Run:
```bash
sh scripts/check-coverage.sh --check
```
Expected: All package floors and total coverage checks PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/ .github/
git commit -m "ci: enforce cross-platform clean-room test execution and invariant verification"
```

---

## Plan Complete and Saved

Plan saved to `docs/superpowers/plans/2026-09-06-hermetic-config-dependency-injection-plan.md`. Two execution options:

1. **Subagent-Driven (recommended)** - Fresh subagent dispatched per task with reviews between tasks.
2. **Inline Execution** - Execute tasks directly in this session with review checkpoints.
