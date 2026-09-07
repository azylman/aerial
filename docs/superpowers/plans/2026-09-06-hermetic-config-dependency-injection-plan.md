# Hermetic Configuration & Pure Constructor Dependency Injection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish hermetic configuration parsing and pure constructor dependency injection across the Aerial monorepo, permanently eliminating ambient environment leakage and fragile `env -u` denylists in test harnesses.

**Architecture:** Consolidate all environment variable and config-file parsing into `brain/pkg/config`. Sub-packages (`brain/pkg/db`, `brain/pkg/memory`, `brain/pkg/gitsync`, `brain/pkg/sanitizer`, `scheduler-mcp`) must never call `os.Getenv` and must receive explicit configuration structs or scalar parameters in constructors. Delete `TEST_DATABASE_URL` entirely. Unit tests across the monorepo must never call `os.Getenv` or `t.Setenv`, instantiating test fixtures directly with pure in-memory or temp SQLite paths. Harness scripts (`scripts/verify.sh` and `scripts/check-coverage.sh`) isolate `go test` with an `env -i` allowlist.

**Tech Stack:** Go 1.24, modernc.org/sqlite, jackc/pgx/v5, gopkg.in/yaml.v3, POSIX sh test harness.

**Spec:** [`docs/superpowers/specs/2026-09-06-hermetic-config-dependency-injection-design.md`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/docs/superpowers/specs/2026-09-06-hermetic-config-dependency-injection-design.md)

## Global Constraints

- Zero `os.Getenv` or `os.LookupEnv` in `brain/pkg/db`, `brain/pkg/memory`, `brain/pkg/gitsync`, `brain/pkg/sanitizer`, `brain/pkg/queue`, `brain/pkg/runner`, `brain/pkg/scheduler`, `brain/pkg/session`, or `brain/pkg/watcher`.
- `brain/pkg/config` is the sole parser of environment variables and configuration files for `brain`.
- `TEST_DATABASE_URL` is completely excised from all code, tests, and scripts.
- Unit tests must never call `os.Getenv` or `t.Setenv`.
- Constructors strictly validate required parameters: `db.InitDB("")` must return error `"db: connection string cannot be empty"`.
- All Go test suites must pass under clean-room `env -i` execution.
- Monorepo Go statement coverage must remain >= 90%.

---

### Task 1: Refactor `brain/pkg/config` for Universal Runtime Configuration

**Files:**
- Modify: `brain/pkg/config/config.go`
- Test: `brain/pkg/config/config_test.go`

**Interfaces:**
- Consumes: Environment variables (`DATABASE_URL`, `POSTGRES_*`, `PORT`, `AGY_BIN`, `GEMINI_API_KEY`, `ANTIGRAVITY_API_KEY`, `SYSTEM_PROMPT`, `DISCORD_TOKEN`, `DISCORD_BOT_TOKEN`, `GITHUB_PAT`, `OLLAMA_URL`, `EMBEDDING_MODEL`, `OLLAMA_EMBEDDING_MODEL`, `EMBEDDING_QUERY_PREFIX`, `CLASSIFIER_MODEL`).
- Produces: Expanded `config.Config` struct with `DatabaseURL`, `Port`, `AgyBin`, `APIKey`, `SystemPrompt`, `DiscordToken`, `GitHubPAT`, `Ollama OllamaConfig`, and `ClassifierModel`. Signature `LoadConfig() (Config, error)` and `DefaultConfig() Config`.

- [ ] **Step 1: Write the failing test**

Add tests to `brain/pkg/config/config_test.go`:
```go
func TestConfig_InfrastructureDefaultsAndResolution(t *testing.T) {
	// Test DefaultConfig contains all infrastructure defaults
	cfg := DefaultConfig()
	if cfg.Port != "8080" {
		t.Errorf("expected default Port '8080', got %q", cfg.Port)
	}
	if cfg.AgyBin != "agy" {
		t.Errorf("expected default AgyBin 'agy', got %q", cfg.AgyBin)
	}
	if cfg.Ollama.BaseURL != "http://localhost:11434" {
		t.Errorf("expected default Ollama.BaseURL 'http://localhost:11434', got %q", cfg.Ollama.BaseURL)
	}
	if cfg.Ollama.Model != "nomic-embed-text" {
		t.Errorf("expected default Ollama.Model 'nomic-embed-text', got %q", cfg.Ollama.Model)
	}
	if cfg.Ollama.QueryPrefix != "search_query: " {
		t.Errorf("expected default Ollama.QueryPrefix 'search_query: ', got %q", cfg.Ollama.QueryPrefix)
	}
	if cfg.ClassifierModel != "gemini-2.5-flash" {
		t.Errorf("expected default ClassifierModel 'gemini-2.5-flash', got %q", cfg.ClassifierModel)
	}
	if cfg.DatabaseURL == "" {
		t.Errorf("expected default DatabaseURL to be populated")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestConfig_InfrastructureDefaultsAndResolution ./brain/pkg/config`
Expected: FAIL with unknown fields (`cfg.Port`, `cfg.AgyBin`, `cfg.Ollama`, etc. undefined).

- [ ] **Step 3: Write minimal implementation**

In `brain/pkg/config/config.go`:
1. Define `OllamaConfig`:
```go
type OllamaConfig struct {
	BaseURL     string `yaml:"base_url" json:"base_url"`
	Model       string `yaml:"model" json:"model"`
	QueryPrefix string `yaml:"query_prefix" json:"query_prefix"`
}
```
2. Add infrastructure fields to `Config`:
```go
type Config struct {
	Model         string                     `yaml:"model" json:"model"`
	Timezone      string                     `yaml:"timezone" json:"timezone"`
	SystemChannel string                     `yaml:"system_channel" json:"system_channel"`
	AdminUsers    []string                   `yaml:"admin_users" json:"admin_users"`
	Channels      map[string]ChannelPolicy   `yaml:"channels" json:"channels"`
	GitSync       GitSyncConfig              `yaml:"git_sync" json:"git_sync"`
	McpServers    map[string]json.RawMessage `yaml:"mcp_servers,omitempty" json:"mcp_servers,omitempty"`

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
```
3. In `DefaultConfig()`: populate the default infrastructure fields:
```go
		DatabaseURL: buildPostgresDSNFromEnv(),
		Port:        "8080",
		AgyBin:      "agy",
		Ollama: OllamaConfig{
			BaseURL:     "http://localhost:11434",
			Model:       "nomic-embed-text",
			QueryPrefix: "search_query: ",
		},
		ClassifierModel: "gemini-2.5-flash",
```
4. In `getFallbackDefaults()`: parse environment variables into `cfg`:
```go
func buildPostgresDSNFromEnv() string {
	if envDSN := os.Getenv("DATABASE_URL"); envDSN != "" {
		return envDSN
	}
	dbUser := os.Getenv("POSTGRES_USER")
	if dbUser == "" {
		dbUser = "aerial"
	}
	dbPass := os.Getenv("POSTGRES_PASSWORD")
	if dbPass == "" {
		dbPass = "aerial_secure_pass"
	}
	dbHost := os.Getenv("POSTGRES_HOST")
	if dbHost == "" {
		dbHost = "postgres"
	}
	dbPort := os.Getenv("POSTGRES_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}
	dbName := os.Getenv("POSTGRES_DB")
	if dbName == "" {
		dbName = "aerial"
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", dbUser, dbPass, dbHost, dbPort, dbName)
}
```
Populate `cfg.DatabaseURL`, `cfg.Port`, `cfg.AgyBin`, `cfg.APIKey`, `cfg.SystemPrompt`, `cfg.DiscordToken`, `cfg.GitHubPAT`, `cfg.Ollama`, `cfg.ClassifierModel`.
5. Refactor `GetTimezone()` and `GetSystemChannel()` to read from `GetRuntimeConfig()` without direct fallback to `os.Getenv`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v -run TestConfig_InfrastructureDefaultsAndResolution ./brain/pkg/config`
Expected: PASS.
Run: `go test -v ./brain/pkg/config`
Expected: PASS with 100% tests passing.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/config/config.go brain/pkg/config/config_test.go
git commit -m "feat(config): consolidate runtime environment parsing and expand Config schema"
```

---

### Task 2: Refactor `brain/pkg/db` to Enforce Pure Constructor Dependency Injection

**Files:**
- Modify: `brain/pkg/db/db.go`
- Test: `brain/pkg/db/db_test.go`

**Interfaces:**
- Consumes: Explicit `dsn string` parameter.
- Produces: `InitDB(dsn string) (*sql.DB, error)`. Returns explicit error `fmt.Errorf("db: connection string cannot be empty")` if `strings.TrimSpace(dsn) == ""`. Deletes `GetDBPath()`.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/db/db_test.go`:
1. Remove `TestGetDBPath_Precedence`.
2. Add `TestInitDB_EmptyDSN_ReturnsError`:
```go
func TestInitDB_EmptyDSN_ReturnsError(t *testing.T) {
	_, err := InitDB("")
	if err == nil {
		t.Fatalf("expected error when dsn is empty, got nil")
	}
	expected := "db: connection string cannot be empty"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
}
```
3. Update `setupTestDB(t *testing.T)`:
```go
func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	database, err := InitDB(dbPath)
	if err != nil {
		t.Fatalf("setupTestDB failed: %v", err)
	}
	return database
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestInitDB_EmptyDSN_ReturnsError ./brain/pkg/db`
Expected: FAIL because `InitDB("")` currently falls back to `GetDBPath()`.

- [ ] **Step 3: Write minimal implementation**

In `brain/pkg/db/db.go`:
1. Delete `GetDBPath()` function completely.
2. In `InitDB(dsn string)`:
```go
func InitDB(dsn string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("db: connection string cannot be empty")
	}
	// ... remainder of connection pool & schema creation ...
}
```
3. Remove unused `os` import if no longer needed in `db.go`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/db`
Expected: PASS. Verify that `git grep "GetDBPath" brain/pkg/db` returns 0 hits and `git grep "TEST_DATABASE_URL" brain/pkg/db` returns 0 hits.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/db/db.go brain/pkg/db/db_test.go
git commit -m "feat(db): enforce non-empty DSN in InitDB and delete GetDBPath"
```

---

### Task 3: Refactor `brain/pkg/memory` for Explicit Client Configuration

**Files:**
- Modify: `brain/pkg/memory/ollama.go`
- Test: `brain/pkg/memory/memory_test.go`

**Interfaces:**
- Consumes: `config.OllamaConfig` from `brain/pkg/config`.
- Produces: `NewClient(cfg config.OllamaConfig) *Client`. Deletes all `os.Getenv` in `ollama.go`.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/memory/memory_test.go`:
Update tests that instantiate `memory.NewClient` to pass `config.OllamaConfig`:
```go
func TestOllamaClient_PureConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(EmbeddingResponse{Embedding: []float32{0.1, 0.2}})
	}))
	defer server.Close()

	cfg := config.OllamaConfig{
		BaseURL:     server.URL,
		Model:       "test-model",
		QueryPrefix: "test_query: ",
	}
	client := NewClient(cfg)
	res, err := client.GenerateEmbedding(context.Background(), "hello", true, 1)
	if err != nil {
		t.Fatalf("GenerateEmbedding failed: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(res))
	}
}
```
Also in `memory_test.go`: remove `os.Getenv("TEST_DATABASE_URL")` from `setupTestDB` in `memory_test.go`, replacing it with `filepath.Join(t.TempDir(), "test.db")`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestOllamaClient_PureConfig ./brain/pkg/memory`
Expected: FAIL due to signature mismatch (`NewClient(string)` vs `NewClient(config.OllamaConfig)`).

- [ ] **Step 3: Write minimal implementation**

In `brain/pkg/memory/ollama.go`:
```go
type Client struct {
	BaseURL     string
	Model       string
	QueryPrefix string
	HTTPClient  *http.Client
}

func NewClient(cfg config.OllamaConfig) *Client {
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = DefaultOllamaURL
	}
	baseURL = strings.TrimSuffix(baseURL, "/")

	model := cfg.Model
	if model == "" {
		model = DefaultEmbeddingModel
	}

	queryPrefix := cfg.QueryPrefix
	if queryPrefix == "" {
		queryPrefix = BGEQueryPrefix
	}

	return &Client{
		BaseURL:     baseURL,
		Model:       model,
		QueryPrefix: queryPrefix,
		HTTPClient: &http.Client{
			Timeout: 3 * time.Second,
		},
	}
}
```
In `GenerateEmbedding`:
- Use `c.QueryPrefix` if `isQuery`: `prompt = c.QueryPrefix + text`.
- Use `c.Model`: `reqBody := EmbeddingRequest{Model: c.Model, Prompt: prompt}`.
- Remove all `os.Getenv("OLLAMA_URL")`, `os.Getenv("EMBEDDING_QUERY_PREFIX")`, `os.Getenv("EMBEDDING_MODEL")`, and `os.Getenv("OLLAMA_EMBEDDING_MODEL")`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/memory`
Expected: PASS. Verify `git grep "os.Getenv" brain/pkg/memory` returns 0 hits.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/memory/ollama.go brain/pkg/memory/memory_test.go
git commit -m "feat(memory): inject OllamaConfig and remove ambient environment lookups"
```

---

### Task 4: Refactor `brain/pkg/gitsync` to Accept Explicit GitHub PAT

**Files:**
- Modify: `brain/pkg/gitsync/gitsync.go`
- Test: `brain/pkg/gitsync/gitsync_test.go`

**Interfaces:**
- Consumes: Explicit `pat string` parameter.
- Produces: `SyncRepo(ctx context.Context, repoPath string, pat string) (bool, error)` and `StartGitSync(ctx context.Context, repos []string, interval time.Duration, pat string, onUpdate func(string))`. Deletes `os.Getenv("GITHUB_PAT")`.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/gitsync/gitsync_test.go`:
Update test call sites to pass `pat string` (e.g. `SyncRepo(ctx, repoPath, "")` or `SyncRepo(ctx, repoPath, "dummy-pat")`).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./brain/pkg/gitsync`
Expected: FAIL due to compilation error (argument count mismatch in `SyncRepo`).

- [ ] **Step 3: Write minimal implementation**

In `brain/pkg/gitsync/gitsync.go`:
1. Change `SyncRepo`:
```go
func SyncRepo(ctx context.Context, repoPath string, pat string) (bool, error) {
    // ...
    // replace: pat := os.Getenv("GITHUB_PAT")
    // with: parameter pat
    authArgs := buildAuthArgs(pat)
    // ...
}
```
2. Change `StartGitSync`:
```go
func StartGitSync(ctx context.Context, repos []string, interval time.Duration, pat string, onUpdate func(string)) {
    // ...
    hasChanges, err := SyncRepo(syncCtx, repo, pat)
    // ...
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/gitsync`
Expected: PASS. Verify `git grep "os.Getenv" brain/pkg/gitsync` returns 0 hits.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/gitsync/gitsync.go brain/pkg/gitsync/gitsync_test.go
git commit -m "feat(gitsync): pass GitHub PAT explicitly into SyncRepo and StartGitSync"
```

---

### Task 5: Refactor `brain/pkg/sanitizer` for Pure Token Registration

**Files:**
- Modify: `brain/pkg/sanitizer/sanitizer.go`
- Test: `brain/pkg/sanitizer/sanitizer_test.go`

**Interfaces:**
- Consumes: Explicit sensitive tokens slice/varargs.
- Produces: `RegisterSensitiveTokens(tokens ...string)`. Sub-package stops scanning `os.Environ()` and `os.Getenv()`.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/sanitizer/sanitizer_test.go`:
```go
func TestRegisterSensitiveTokens_Hermetic(t *testing.T) {
	RegisterSensitiveTokens("super-secret-token-12345", "another_secret_password")
	text := "Log with super-secret-token-12345 in output"
	sanitized := SanitizeLog(text)
	if strings.Contains(sanitized, "super-secret-token-12345") {
		t.Errorf("expected secret token to be redacted, got %q", sanitized)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestRegisterSensitiveTokens_Hermetic ./brain/pkg/sanitizer`
Expected: FAIL (`RegisterSensitiveTokens` undefined).

- [ ] **Step 3: Write minimal implementation**

In `brain/pkg/sanitizer/sanitizer.go`:
1. Add `RegisterSensitiveTokens`:
```go
func RegisterSensitiveTokens(tokens ...string) {
	secretsMu.Lock()
	defer secretsMu.Unlock()
	for _, tok := range tokens {
		val := strings.TrimSpace(tok)
		if len(val) >= 4 {
			lower := strings.ToLower(val)
			if lower != "true" && lower != "false" && lower != "enabled" && lower != "disabled" {
				cachedSecrets = append(cachedSecrets, val)
			}
		}
	}
	sortSecrets(cachedSecrets)
}
```
2. In `buildEnvSecrets()`: remove direct calls to `os.Getenv` and `os.Environ()`. Keep pattern-based URL/credential sanitization in `SanitizeLog`.
3. In `sanitizer_test.go`: replace any `t.Setenv` with direct calls to `RegisterSensitiveTokens`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/sanitizer`
Expected: PASS. Verify `git grep "os.Getenv" brain/pkg/sanitizer` returns 0 hits.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/sanitizer/sanitizer.go brain/pkg/sanitizer/sanitizer_test.go
git commit -m "feat(sanitizer): allow explicit token registration and remove ambient env scan"
```

---

### Task 6: Refactor `scheduler-mcp` for Explicit Configuration & Hermetic Tests

**Files:**
- Modify: `scheduler-mcp/db.go`
- Modify: `scheduler-mcp/main.go`
- Modify: `scheduler-mcp/tools.go`
- Test: `scheduler-mcp/server_test.go`
- Test: `scheduler-mcp/tools_test.go`

**Interfaces:**
- Consumes: Explicit `dsn string` in `InitDB(dsn string)` and explicit `timezone string` in `tools.go`.
- Produces: Deletes `GetDBPath()`. `main.go` parses env once at startup. Tests use SQLite temp paths without environment variables.

- [ ] **Step 1: Write the failing test**

In `scheduler-mcp/server_test.go`:
Update `setupTestDB`:
```go
func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test_scheduler.db")
	database, err := InitDB(dbPath)
	if err != nil {
		t.Fatalf("setupTestDB failed: %v", err)
	}
	return database
}
```
Remove `GetDBPath` tests that test environment variable combinations.
Add `TestInitDB_EmptyDSN_ReturnsError`:
```go
func TestInitDB_EmptyDSN_ReturnsError(t *testing.T) {
	_, err := InitDB("")
	if err == nil {
		t.Fatalf("expected error with empty dsn, got nil")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestInitDB_EmptyDSN_ReturnsError ./scheduler-mcp`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

1. In `scheduler-mcp/db.go`:
   - Delete `GetDBPath()`.
   - In `InitDB(dsn string)`: require `strings.TrimSpace(dsn) != ""`, returning error if empty.
2. In `scheduler-mcp/main.go`:
   - Build `dsn` from `DATABASE_URL` / `POSTGRES_*` in `main()`.
   - Read `PORT` (default `"8080"`).
   - Read `timezone` from `DEFAULT_TIMEZONE` / `TZ` (default `"America/Los_Angeles"`).
   - Pass `dsn` to `InitDB(dsn)` and configure default timezone on tools.
3. In `scheduler-mcp/tools.go`:
   - Delete `os.Getenv("DEFAULT_TIMEZONE")` and `os.Getenv("TZ")` from `GetDefaultTimezone()`. Store default timezone in a package-level variable or struct initialized by `main.go`.
4. In `scheduler-mcp/tools_test.go`:
   - Remove `os.Getenv("TEST_DATABASE_URL")` and all `t.Setenv` calls.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./scheduler-mcp`
Expected: PASS. Verify `git grep "TEST_DATABASE_URL" scheduler-mcp` returns 0 hits.

- [ ] **Step 5: Commit**

```bash
git add scheduler-mcp/db.go scheduler-mcp/main.go scheduler-mcp/tools.go scheduler-mcp/server_test.go scheduler-mcp/tools_test.go
git commit -m "feat(scheduler-mcp): remove GetDBPath, enforce non-empty dsn, and hermeticize tests"
```

---

### Task 7: Refactor `brain/main.go` and `brain/main_test.go` to Pure Composition Root

**Files:**
- Modify: `brain/main.go`
- Modify: `brain/pkg/scheduler/scheduler_test.go`
- Test: `brain/main_test.go`

**Interfaces:**
- Consumes: Strongly-typed `config.Config` from `config.LoadConfig()`.
- Produces: `NewBrainConfig(cfg config.Config) BrainConfig`. Wires dependencies (`db.InitDB`, `memory.NewClient`, `sanitizer.RegisterSensitiveTokens`, `gitsync.SyncRepo`) directly from `cfg`.

- [ ] **Step 1: Write the failing test**

In `brain/main_test.go`:
Update `TestInitializeBrainEnvironment_And_Config` and `TestRunBrainApp_Lifecycle`:
```go
func TestNewBrainConfig_FromConfig(t *testing.T) {
	cfg := config.Config{
		Model:           "gemini-2.5-flash",
		Port:            "9090",
		AgyBin:          "/bin/true",
		APIKey:          "test-key",
		SystemPrompt:    "test-prompt",
		DatabaseURL:     ":memory:",
		DiscordToken:    "test-token",
		GitHubPAT:       "test-pat",
		ClassifierModel: "gemini-2.5-flash",
	}
	bCfg := NewBrainConfig(cfg)
	if bCfg.Port != "9090" || bCfg.DBPath != ":memory:" {
		t.Errorf("unexpected BrainConfig: %+v", bCfg)
	}
}
```
Remove `t.Setenv` from `TestRunBrainApp_Lifecycle` in `main_test.go`.
In `brain/pkg/scheduler/scheduler_test.go`: remove `t.Setenv("DEFAULT_TIMEZONE", ...)` and replace with direct test cases against `config.LoadConfigFromPaths`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestNewBrainConfig_FromConfig ./brain`
Expected: FAIL (`NewBrainConfig` undefined).

- [ ] **Step 3: Write minimal implementation**

In `brain/main.go`:
1. Rename `NewBrainConfigFromEnv` to `NewBrainConfig`:
```go
func NewBrainConfig(cfg config.Config) BrainConfig {
	return BrainConfig{
		Port:            cfg.Port,
		AgyBin:          cfg.AgyBin,
		APIKey:          cfg.APIKey,
		SystemPrompt:    cfg.SystemPrompt,
		Model:           cfg.Model,
		DBPath:          cfg.DatabaseURL,
		DiscordToken:    cfg.DiscordToken,
		ClassifierModel: cfg.ClassifierModel,
	}
}
```
2. In `RunBrainApp(ctx context.Context, bCfg BrainConfig)`:
   - If `bCfg.DBPath == ""`, return error `fmt.Errorf("db path cannot be empty")`.
   - Call `database, err := db.InitDB(bCfg.DBPath)`.
   - Call `sanitizer.RegisterSensitiveTokens(bCfg.APIKey, bCfg.DiscordToken)`.
   - Use `bCfg.ClassifierModel` for ambient classifier.
3. In `main()`:
   - Call `cfg, err := config.LoadConfig()`.
   - Call `sanitizer.RegisterSensitiveTokens(cfg.APIKey, cfg.DiscordToken, cfg.GitHubPAT)`.
   - Initialize `memory.NewClient(cfg.Ollama)`.
   - Call `bCfg := NewBrainConfig(cfg)`.
   - Pass `bCfg` to `RunBrainApp`.
   - In `gitsync` invocation: pass `cfg.GitHubPAT` to `gitsync.SyncRepo`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain`
Expected: PASS.
Run: `go test -v ./brain/pkg/scheduler`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/main.go brain/main_test.go brain/pkg/scheduler/scheduler_test.go
git commit -m "feat(brain): consume config.Config directly in composition root and eliminate ambient env"
```

---

### Task 8: Implement Clean-Room Test Execution in Monorepo Scripts & Verify

**Files:**
- Modify: `scripts/verify.sh`
- Modify: `scripts/check-coverage.sh`

**Interfaces:**
- Consumes: Host environment.
- Produces: Clean-room test execution under `env -i` allowlist: `PATH`, `HOME`, `GOROOT`, `GOPATH`, `GOCACHE`, `TMPDIR`, `CGO_ENABLED`.

- [ ] **Step 1: Write the failing test / inspection**

Run `git grep "env -u" scripts/` to identify fragile denylist usages.
Expected: Found in `scripts/verify.sh` and `scripts/check-coverage.sh`.

- [ ] **Step 2: Write minimal implementation**

1. In `scripts/verify.sh`:
Replace line 83:
```sh
run_go_test() {
    svc="$1"
    if [ -d "$svc" ]; then
        echo "   [go test] Testing $svc (clean room)..."
        if has_cmd go; then
            (cd "$svc" && env -i PATH="$PATH" HOME="$HOME" GOROOT="${GOROOT:-}" GOPATH="${GOPATH:-}" GOCACHE="${GOCACHE:-}" TMPDIR="${TMPDIR:-/tmp}" CGO_ENABLED="${CGO_ENABLED:-1}" go test -v -p 1 ./...)
        elif has_cmd docker; then
            docker run --rm -v "$(pwd)/$svc:/app" -w /app golang:1.24 go test -v -p 1 ./...
        else
            echo "🚨 [Aerial Verify] Error: Neither go nor docker found in PATH." >&2
            exit 1
        fi
    fi
}
```
2. In `scripts/check-coverage.sh`:
Replace line 152:
```sh
        (cd "$svc" && env -i PATH="$PATH" HOME="$HOME" GOROOT="${GOROOT:-}" GOPATH="${GOPATH:-}" GOCACHE="${GOCACHE:-}" TMPDIR="${TMPDIR:-/tmp}" CGO_ENABLED="${CGO_ENABLED:-1}" go test -coverprofile="$prof_file" "$pkg" >/dev/null 2>&1) || true
```
3. Run monorepo static analysis check:
- `git grep "os.Getenv" brain/pkg/ | grep -v "brain/pkg/config"` must return 0.
- `git grep "TEST_DATABASE_URL"` must return 0 across the entire repository.
- `git grep "GetDBPath"` must return 0 across the entire repository.

- [ ] **Step 3: Run verification and coverage gates**

Run: `sh scripts/verify.sh --full`
Expected: All checks pass 100%.
Run: `sh scripts/check-coverage.sh --check`
Expected: All statement coverage thresholds pass with monorepo coverage >= 90%.

- [ ] **Step 4: Commit**

```bash
git add scripts/verify.sh scripts/check-coverage.sh
git commit -m "feat(scripts): replace fragile env denylists with clean-room allowlist test execution"
```
