# Hermetic Configuration & Pure Constructor Dependency Injection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish hermetic configuration parsing and pure constructor dependency injection across the Aerial monorepo, permanently eliminating ambient environment leakage, fragile `env -u` denylists, and toxic postgres fallback traps in test harnesses.

**Architecture:** Consolidate all environment variable and config-file parsing into `brain/pkg/config`. Sub-packages (`brain/pkg/db`, `brain/pkg/memory`, `brain/pkg/gitsync`, `brain/pkg/sanitizer`, `brain/pkg/scheduler`, `scheduler-mcp`) must never call `os.Getenv` or `config.GetEnv` and must receive explicit configuration structs or scalar parameters in constructors. Delete `TEST_DATABASE_URL` entirely across the repo. Unit tests across the monorepo must never call `os.Getenv` or `t.Setenv` for infrastructure credentials, instantiating test fixtures directly with pure in-memory or temp SQLite paths. Harness scripts (`scripts/verify.sh` and `scripts/check-coverage.sh`) execute tests under an OS-aware clean-room allowlist.

**Tech Stack:** Go 1.24, modernc.org/sqlite, jackc/pgx/v5, gopkg.in/yaml.v3, POSIX sh test harness.

**Spec:** [`docs/superpowers/specs/2026-09-06-hermetic-config-dependency-injection-design.md`](file:///C:/Users/alexz/.gemini/antigravity/scratch/gundam/docs/superpowers/specs/2026-09-06-hermetic-config-dependency-injection-design.md)

## Global Constraints

- Zero `os.Getenv` or `os.LookupEnv` in `brain/pkg/db`, `brain/pkg/memory`, `brain/pkg/gitsync`, `brain/pkg/sanitizer`, `brain/pkg/queue`, `brain/pkg/runner`, `brain/pkg/scheduler`, `brain/pkg/session`, or `brain/pkg/watcher`.
- `brain/pkg/config` is the sole parser of environment variables and configuration files for `brain`. `config.GetEnv` is unexported to `getEnv` so subpackages cannot access it.
- `TEST_DATABASE_URL` is completely excised from all code, tests, scripts, and workflows.
- `DefaultConfig()` and `buildPostgresDSNFromEnv()` must never default to hostname `postgres:5432` when `POSTGRES_HOST` is unset; local development and test defaults must use SQLite or an explicit path.
- Unit tests must never call `os.Getenv` or `t.Setenv` for infrastructure/credentials. (Only `config_test.go` and `dashboard/main_test.go` retain scoped `t.Setenv` to test fallback parsing logic).
- Constructors strictly validate required parameters: `db.InitDB("")` must return error `"db: connection string cannot be empty"`. Unrecognized URL schemes return errors instead of silently falling through to SQLite.
- All Go test suites must compile and pass under clean-room execution at every commit.
- Monorepo Go statement coverage must satisfy all package floors and threshold checks.

---

### Task 1: Refactor `brain/pkg/config` for Universal Runtime Configuration & Safe Defaults

**Files:**
- Modify: `brain/pkg/config/config.go`
- Test: `brain/pkg/config/config_test.go`

**Interfaces:**
- Consumes: Environment variables (`DATABASE_URL`, `POSTGRES_*`, `PORT`, `AGY_BIN`, `GEMINI_API_KEY`, `ANTIGRAVITY_API_KEY`, `SYSTEM_PROMPT`, `DISCORD_TOKEN`, `DISCORD_BOT_TOKEN`, `GITHUB_PAT`, `OLLAMA_URL`, `EMBEDDING_MODEL`, `OLLAMA_EMBEDDING_MODEL`, `EMBEDDING_QUERY_PREFIX`, `CLASSIFIER_MODEL`, `AMBIENT_CLASSIFIER_MODEL`).
- Produces: Expanded `config.Config` struct with `DatabaseURL`, `Port`, `AgyBin`, `APIKey`, `SystemPrompt`, `DiscordToken`, `GitHubPAT`, `Ollama OllamaConfig`, and `ClassifierModel`. Unexports `getEnv`. In `LoadConfigFromPaths`, inherits missing fields from `fallback`.

- [ ] **Step 1: Write the failing test**

Add tests to `brain/pkg/config/config_test.go`:
```go
func TestConfig_InfrastructureDefaultsAndResolution(t *testing.T) {
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
	if cfg.ClassifierModel != "Gemini 3.8 Flash (Low)" {
		t.Errorf("expected default ClassifierModel 'Gemini 3.8 Flash (Low)', got %q", cfg.ClassifierModel)
	}
}

func TestConfig_LoadConfigFromPaths_MergesFallbackInfrastructure(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	// Config file omitting database_url, port, agy_bin, etc.
	yamlContent := "model: 'custom-model'\nchannels:\n  default:\n    mode: 'threads'\n"
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	loaded, err := LoadConfigFromPaths(cfgPath)
	if err != nil {
		t.Fatalf("LoadConfigFromPaths failed: %v", err)
	}
	if loaded.Port != "8080" {
		t.Errorf("expected merged Port '8080', got %q", loaded.Port)
	}
	if loaded.AgyBin != "agy" {
		t.Errorf("expected merged AgyBin 'agy', got %q", loaded.AgyBin)
	}
	if loaded.ClassifierModel != "Gemini 3.8 Flash (Low)" {
		t.Errorf("expected merged ClassifierModel 'Gemini 3.8 Flash (Low)', got %q", loaded.ClassifierModel)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run "TestConfig_InfrastructureDefaultsAndResolution|TestConfig_LoadConfigFromPaths_MergesFallbackInfrastructure" ./brain/pkg/config`
Expected: FAIL with unknown fields.

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
3. Implement `buildPostgresDSNFromEnv()` using `net/url` and `net.JoinHostPort`:
```go
func buildPostgresDSNFromEnv() string {
	if envDSN := strings.TrimSpace(os.Getenv("DATABASE_URL")); envDSN != "" {
		return envDSN
	}
	dbHost := strings.TrimSpace(os.Getenv("POSTGRES_HOST"))
	if dbHost == "" {
		// Do not default to postgres:5432 if POSTGRES_HOST is unset! Return empty string.
		return ""
	}
	dbUser := os.Getenv("POSTGRES_USER")
	if dbUser == "" {
		dbUser = "aerial"
	}
	dbPass := os.Getenv("POSTGRES_PASSWORD")
	if dbPass == "" {
		dbPass = "aerial_secure_pass"
	}
	dbPort := os.Getenv("POSTGRES_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}
	dbName := os.Getenv("POSTGRES_DB")
	if dbName == "" {
		dbName = "aerial"
	}
	sslMode := os.Getenv("POSTGRES_SSLMODE")
	if sslMode == "" {
		sslMode = "disable"
	}

	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(dbUser, dbPass),
		Host:     net.JoinHostPort(dbHost, dbPort),
		Path:     "/" + dbName,
		RawQuery: "sslmode=" + url.QueryEscape(sslMode),
	}
	return u.String()
}
```
4. In `DefaultConfig()`: populate static pure defaults without calling `os.Getenv`:
```go
func DefaultConfig() Config {
	defaultIgnoreBots := true
	return Config{
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
		McpServers: make(map[string]json.RawMessage),
		Port:       "8080",
		AgyBin:     "agy",
		Ollama: OllamaConfig{
			BaseURL:     "http://localhost:11434",
			Model:       "nomic-embed-text",
			QueryPrefix: "search_query: ",
		},
		ClassifierModel: "Gemini 3.8 Flash (Low)",
	}
}
```
5. In `getFallbackDefaults()`: parse environment variables into `cfg`:
```go
	cfg.DatabaseURL = buildPostgresDSNFromEnv()
	if p := getEnv("PORT", ""); p != "" { cfg.Port = p }
	if b := getEnv("AGY_BIN", ""); b != "" { cfg.AgyBin = b }
	if k := getEnv("GEMINI_API_KEY", getEnv("ANTIGRAVITY_API_KEY", "")); k != "" { cfg.APIKey = k }
	if sp := getEnv("SYSTEM_PROMPT", ""); sp != "" { cfg.SystemPrompt = sp }
	if tok := getEnv("DISCORD_TOKEN", getEnv("DISCORD_BOT_TOKEN", "")); tok != "" { cfg.DiscordToken = tok }
	if pat := getEnv("GITHUB_PAT", ""); pat != "" { cfg.GitHubPAT = pat }
	if oURL := getEnv("OLLAMA_URL", ""); oURL != "" { cfg.Ollama.BaseURL = oURL }
	if oModel := getEnv("EMBEDDING_MODEL", getEnv("OLLAMA_EMBEDDING_MODEL", "")); oModel != "" { cfg.Ollama.Model = oModel }
	if oPref := getEnv("EMBEDDING_QUERY_PREFIX", ""); oPref != "" { cfg.Ollama.QueryPrefix = oPref }
	if cls := getEnv("CLASSIFIER_MODEL", getEnv("AMBIENT_CLASSIFIER_MODEL", "")); cls != "" { cfg.ClassifierModel = cls }
```
6. In `LoadConfigFromPaths`: merge missing infrastructure fields from `fallback`:
```go
	if strings.TrimSpace(parsed.DatabaseURL) == "" {
		parsed.DatabaseURL = fallback.DatabaseURL
	}
	if strings.TrimSpace(parsed.Port) == "" {
		parsed.Port = fallback.Port
	}
	if strings.TrimSpace(parsed.AgyBin) == "" {
		parsed.AgyBin = fallback.AgyBin
	}
	if strings.TrimSpace(parsed.APIKey) == "" {
		parsed.APIKey = fallback.APIKey
	}
	if strings.TrimSpace(parsed.SystemPrompt) == "" {
		parsed.SystemPrompt = fallback.SystemPrompt
	}
	if strings.TrimSpace(parsed.DiscordToken) == "" {
		parsed.DiscordToken = fallback.DiscordToken
	}
	if strings.TrimSpace(parsed.GitHubPAT) == "" {
		parsed.GitHubPAT = fallback.GitHubPAT
	}
	if strings.TrimSpace(parsed.Ollama.BaseURL) == "" {
		parsed.Ollama = fallback.Ollama
	}
	if strings.TrimSpace(parsed.ClassifierModel) == "" {
		parsed.ClassifierModel = fallback.ClassifierModel
	}
```
7. Unexport `GetEnv` to `getEnv(key, defaultVal string) string`.
8. Refactor `LoadMCPConfig(pat string)` to take `pat` explicitly from caller rather than `os.Getenv("GITHUB_PAT")`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/config`
Expected: PASS with 100% test success.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/config/config.go brain/pkg/config/config_test.go
git commit -m "feat(config): consolidate runtime environment parsing and unexport getEnv"
```

---

### Task 2: Refactor `brain/pkg/db` and Update Call Sites Atomically

**Files:**
- Modify: `brain/pkg/db/db.go`
- Modify: `brain/pkg/db/db_test.go`
- Modify: `brain/main.go`
- Modify: `brain/main_test.go`

**Interfaces:**
- Consumes: Explicit `dsn string` parameter.
- Produces: `InitDB(dsn string) (*sql.DB, error)`. Returns explicit error `fmt.Errorf("db: connection string cannot be empty")` if `strings.TrimSpace(dsn) == ""`. Disallows unsupported schemes. Sets `SetMaxOpenConns(1)` for `:memory:`. Deletes `GetDBPath()` and all its call sites atomically so `brain` compiles cleanly.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/db/db_test.go`:
1. Remove `TestGetDBPath_Precedence` and `TestDBPath`.
2. Add `TestInitDB_ValidationAndDrivers`:
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

func TestInitDB_UnsupportedScheme_ReturnsError(t *testing.T) {
	_, err := InitDB("mysql://user:pass@localhost:3306/db")
	if err == nil {
		t.Fatalf("expected error for unsupported scheme, got nil")
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
4. In `brain/main_test.go`: update line 1413 in `TestRunBrainApp_ErrorBranches` to verify that `DBPath: ""` immediately returns an error.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run "TestInitDB_EmptyDSN_ReturnsError|TestInitDB_UnsupportedScheme_ReturnsError" ./brain/pkg/db`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

1. In `brain/pkg/db/db.go`:
   - Delete `GetDBPath()`.
   - Update `InitDB(dsn string)`:
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
		if _, err := stdlib.ParseConfig(trimmed); err != nil {
			return nil, fmt.Errorf("db: invalid postgres connection string: %w", err)
		}
		// ... retry loop, connection tuning, advisory lock, schema creation ...
	} else {
		// SQLite: constrain connections for in-memory databases
		if trimmed == ":memory:" || strings.Contains(trimmed, "mode=memory") {
			database.SetMaxOpenConns(1)
		}
	}
	return database, nil
}
```
2. In `brain/main.go`:
   - Line 843: `DBPath: cfg.DatabaseURL,`
   - Line 928-931:
```go
	dbPath := strings.TrimSpace(bCfg.DBPath)
	if dbPath == "" {
		return fmt.Errorf("db path cannot be empty")
	}
	database, err := db.InitDB(dbPath)
```
   - Line 1045: `database, err := db.InitDB(cfg.DatabaseURL)`

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/db`
Expected: PASS.
Run: `go test -v -run TestRunBrainApp_ErrorBranches ./brain`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/db/db.go brain/pkg/db/db_test.go brain/main.go brain/main_test.go
git commit -m "feat(db): enforce strict DSN validation, delete GetDBPath, and update callers"
```

---

### Task 3: Refactor `brain/pkg/memory` and Update Call Sites Atomically

**Files:**
- Modify: `brain/pkg/memory/ollama.go`
- Modify: `brain/pkg/memory/memory_test.go`
- Modify: `brain/pkg/queue/queue.go`
- Modify: `brain/pkg/scheduler/scheduler.go`
- Modify: `brain/pkg/scheduler/scheduler_test.go`

**Interfaces:**
- Consumes: `memory.ClientConfig` (decoupled from `config`).
- Produces: `NewClient(cfg memory.ClientConfig) *Client`. Deletes all `os.Getenv` in `ollama.go`. Updates `queue.go`, `scheduler.go`, and test suites so all callers compile and pass cleanly.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/memory/memory_test.go`:
1. Remove `os.Getenv("TEST_DATABASE_URL")` from `setupTestDB`.
2. Add `TestOllamaClient_PureConfig`:
```go
func TestOllamaClient_PureConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(EmbeddingResponse{Embedding: []float32{0.1, 0.2}})
	}))
	defer server.Close()

	cfg := ClientConfig{
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

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestOllamaClient_PureConfig ./brain/pkg/memory`
Expected: FAIL due to signature mismatch.

- [ ] **Step 3: Write minimal implementation**

1. In `brain/pkg/memory/ollama.go`:
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

func NewClient(cfg ClientConfig) *Client {
	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	if baseURL == "" {
		baseURL = DefaultOllamaURL
	}
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
		HTTPClient:  &http.Client{Timeout: 3 * time.Second},
	}
}
```
In `GenerateEmbedding`: use `c.QueryPrefix` when `isQuery` is true, and use `c.Model`. Remove all `os.Getenv`.
2. In `brain/pkg/queue/queue.go:257`:
```go
	if cfg.MemoryClient == nil {
		cfg.MemoryClient = memory.NewClient(memory.ClientConfig{})
	}
```
3. In `brain/pkg/scheduler/scheduler.go:362`:
```go
	if client == nil {
		client = memory.NewClient(memory.ClientConfig{})
	}
```
4. In `brain/pkg/scheduler/scheduler_test.go:1336`:
```go
	client := memory.NewClient(memory.ClientConfig{})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/memory`
Expected: PASS.
Run: `go test -v -run TestScheduleManager_FactExtraction ./brain/pkg/scheduler`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/memory/ollama.go brain/pkg/memory/memory_test.go brain/pkg/queue/queue.go brain/pkg/scheduler/scheduler.go brain/pkg/scheduler/scheduler_test.go
git commit -m "feat(memory): decouple ClientConfig, remove ambient env lookups, and update callers"
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
Update all `SyncRepo(ctx, repoPath)` calls to `SyncRepo(ctx, repoPath, "")` or `SyncRepo(ctx, repoPath, "test-pat")`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./brain/pkg/gitsync`
Expected: FAIL with argument count mismatch.

- [ ] **Step 3: Write minimal implementation**

In `brain/pkg/gitsync/gitsync.go`:
1. Change `SyncRepo`:
```go
func SyncRepo(ctx context.Context, repoPath string, pat string) (bool, error) {
    // ...
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
Expected: PASS. Verify `git grep "os.Getenv" brain/pkg/gitsync` returns 0.

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
- Produces: `RegisterSensitiveTokens(tokens ...string)` and `ResetSensitiveTokens()`. Uses real identifiers `envMu` and `envSecretsCache`.

- [ ] **Step 1: Write the failing test**

In `brain/pkg/sanitizer/sanitizer_test.go`:
```go
func TestRegisterSensitiveTokens_Hermetic(t *testing.T) {
	t.Cleanup(ResetSensitiveTokens)
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
```go
func RegisterSensitiveTokens(tokens ...string) {
	envMu.Lock()
	defer envMu.Unlock()
	for _, tok := range tokens {
		val := strings.TrimSpace(tok)
		if len(val) >= 4 {
			lower := strings.ToLower(val)
			if lower != "true" && lower != "false" && lower != "enabled" && lower != "disabled" {
				envSecretsCache = append(envSecretsCache, val)
			}
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
Remove direct `os.Getenv` iteration from `buildEnvSecrets()`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain/pkg/sanitizer`
Expected: PASS. Verify `git grep "os.Getenv" brain/pkg/sanitizer` returns 0.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/sanitizer/sanitizer.go brain/pkg/sanitizer/sanitizer_test.go
git commit -m "feat(sanitizer): allow explicit token registration and reset with real mutex identifiers"
```

---

### Task 6: Refactor `scheduler-mcp` for Explicit Configuration & Isolation

**Files:**
- Modify: `scheduler-mcp/db.go`
- Modify: `scheduler-mcp/main.go`
- Modify: `scheduler-mcp/tools.go`
- Test: `scheduler-mcp/server_test.go`
- Test: `scheduler-mcp/tools_test.go`

**Interfaces:**
- Consumes: Explicit `dsn string` in `InitDB(dsn string)` and explicit `timezone string` in `ToolHandler`.
- Produces: Deletes `GetDBPath()`. Acquires `pg_advisory_lock(849201948201)` during PostgreSQL migrations. Sets `SetMaxOpenConns(1)` for `:memory:`. Deletes `TestPostgresSchedules`.

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
Add `TestInitDB_EmptyDSN_ReturnsError`:
```go
func TestInitDB_EmptyDSN_ReturnsError(t *testing.T) {
	_, err := InitDB("")
	if err == nil {
		t.Fatalf("expected error with empty dsn, got nil")
	}
}
```
In `scheduler-mcp/tools_test.go`: delete `TestPostgresSchedules` (lines 474-565) which queried `TEST_DATABASE_URL`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestInitDB_EmptyDSN_ReturnsError ./scheduler-mcp`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

1. In `scheduler-mcp/db.go`:
   - Delete `GetDBPath()`.
   - In `InitDB(dsn string)`: return error if empty.
   - For PostgreSQL: acquire and release `SELECT pg_advisory_lock(849201948201)`.
   - For SQLite: if `dsn == ":memory:" || strings.Contains(dsn, "mode=memory")`, set `database.SetMaxOpenConns(1)`.
2. In `scheduler-mcp/tools.go`:
   - Add `defaultTimezone string` to `ToolHandler`:
```go
type ToolHandler struct {
	db              *sql.DB
	defaultTimezone string
}

func NewToolHandler(database *sql.DB, defaultTimezone string) *ToolHandler {
	if defaultTimezone == "" {
		defaultTimezone = "America/Los_Angeles"
	}
	return &ToolHandler{db: database, defaultTimezone: defaultTimezone}
}
```
   - Delete `os.Getenv("DEFAULT_TIMEZONE")` and `os.Getenv("TZ")` from `GetDefaultTimezone()`. Use `h.defaultTimezone`.
3. In `scheduler-mcp/main.go`:
   - Parse `PORT`, `DATABASE_URL` / `POSTGRES_*`, `DEFAULT_TIMEZONE` / `TZ` in `main()`.
   - Pass `dsn` to `InitDB` and `timezone` to `NewToolHandler`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./scheduler-mcp`
Expected: PASS. Verify `git grep "TEST_DATABASE_URL" scheduler-mcp` returns 0.

- [ ] **Step 5: Commit**

```bash
git add scheduler-mcp/db.go scheduler-mcp/main.go scheduler-mcp/tools.go scheduler-mcp/server_test.go scheduler-mcp/tools_test.go
git commit -m "feat(scheduler-mcp): remove GetDBPath, add advisory lock, inject timezone, and hermeticize tests"
```

---

### Task 7: Refactor `brain/pkg/scheduler` and `brain/main.go` Composition Root

**Files:**
- Modify: `brain/pkg/scheduler/scheduler.go`
- Modify: `brain/pkg/scheduler/scheduler_test.go`
- Modify: `brain/main.go`
- Test: `brain/main_test.go`

**Interfaces:**
- Consumes: Strongly-typed `config.Config` from `config.LoadConfig()`.
- Produces: `NewBrainConfig(cfg config.Config) BrainConfig` with `MemoryClient` and `ClassifierModel`. Eliminates `config.GetEnv` from `ExtractFactsLLM`. Wires `sanitizer.RegisterSensitiveTokens` on startup and hot-reload.

- [ ] **Step 1: Write the failing test**

In `brain/main_test.go`:
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
		Ollama: config.OllamaConfig{
			BaseURL: "http://localhost:11434",
		},
	}
	bCfg := NewBrainConfig(cfg)
	if bCfg.Port != "9090" || bCfg.DBPath != ":memory:" {
		t.Errorf("unexpected BrainConfig: %+v", bCfg)
	}
}
```
In `brain/pkg/scheduler/scheduler_test.go`: remove `t.Setenv("AGY_BIN", ...)` and `t.Setenv("DEFAULT_TIMEZONE", ...)`. Pass explicit arguments to `ExtractFactsLLM`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestNewBrainConfig_FromConfig ./brain`
Expected: FAIL.

- [ ] **Step 3: Write minimal implementation**

1. In `brain/pkg/scheduler/scheduler.go`:
   - Refactor `ExtractFactsLLM`:
```go
func ExtractFactsLLM(ctx context.Context, prompt, agyBin, apiKey, model string) (string, error) {
	if agyBin == "" {
		agyBin = "agy"
	}
	if model == "" {
		model = "gemini-2.5-flash"
	}
	stdout, _, exitCode, err := runner.RunAgy(ctx, agyBin, prompt, "", apiKey, model, 5)
	if exitCode != 0 || err != nil {
		return "", fmt.Errorf("agy fact extraction exitCode=%d err=%v", exitCode, err)
	}
	resp, parseErr := runner.ParseAgyOutput(stdout)
	if parseErr != nil {
		return "", fmt.Errorf("failed to parse agy json output in scheduler: %w (raw: %q)", parseErr, stdout)
	}
	return resp.Response, nil
}
```
2. In `brain/main.go`:
   - Rename `NewBrainConfigFromEnv` to `NewBrainConfig`:
```go
type BrainConfig struct {
	Port            string
	AgyBin          string
	APIKey          string
	SystemPrompt    string
	Model           string
	DBPath          string
	DiscordToken    string
	ClassifierModel string
	Ollama          config.OllamaConfig
	GitHubPAT       string
}

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
		Ollama:          cfg.Ollama,
		GitHubPAT:       cfg.GitHubPAT,
	}
}
```
   - In `RunBrainApp(ctx context.Context, bCfg BrainConfig)`:
     - Register sensitive tokens: `sanitizer.RegisterSensitiveTokens(bCfg.APIKey, bCfg.DiscordToken, bCfg.GitHubPAT)`
     - Initialize `memClient`:
```go
	memClient := memory.NewClient(memory.ClientConfig{
		BaseURL:     bCfg.Ollama.BaseURL,
		Model:       bCfg.Ollama.Model,
		QueryPrefix: bCfg.Ollama.QueryPrefix,
	})
```
     - Pass `memClient` into `queue.WorkerPoolConfig{..., MemoryClient: memClient}`.
     - Pass `memClient` and fact extraction closure `func(ctx context.Context, prompt string) (string, error) { return scheduler.ExtractFactsLLM(ctx, prompt, bCfg.AgyBin, bCfg.APIKey, bCfg.Model) }` to `scheduler.Start`.
   - In `CreateReloadConfigFunc`:
     - On reload success, call `sanitizer.RegisterSensitiveTokens(latestCfg.APIKey, latestCfg.DiscordToken, latestCfg.GitHubPAT)`.
     - Load MCP config with explicit token: `config.LoadMCPConfig(latestCfg.GitHubPAT)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v ./brain`
Expected: PASS.
Run: `go test -v ./brain/pkg/scheduler`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/scheduler/scheduler.go brain/pkg/scheduler/scheduler_test.go brain/main.go brain/main_test.go
git commit -m "feat(brain): wire pure composition root and eliminate config.GetEnv in scheduler"
```

---

### Task 8: Implement Cross-Platform Clean-Room Harness & Purge Monorepo Residuals

**Files:**
- Modify: `scripts/verify.sh`
- Modify: `scripts/check-coverage.sh`
- Modify: `scripts/migrate_sqlite_to_postgres.go`
- Modify: `.github/workflows/docker-publish.yml`

**Interfaces:**
- Consumes: Host and CI test environments.
- Produces: OS-aware `run_clean_test` allowlist supporting Windows, macOS, and Linux without credential leakage. Eliminates remaining references to `TEST_DATABASE_URL`.

- [ ] **Step 1: Check residual `TEST_DATABASE_URL` references**

Run: `git grep "TEST_DATABASE_URL"`
Expected: Found in `scripts/migrate_sqlite_to_postgres.go:81` and `.github/workflows/docker-publish.yml:97`.

- [ ] **Step 2: Write minimal implementation**

1. In `scripts/migrate_sqlite_to_postgres.go`: remove lines 80-82 (`TEST_DATABASE_URL` check).
2. In `.github/workflows/docker-publish.yml`: remove line 97 (`TEST_DATABASE_URL`).
3. In `scripts/verify.sh`: define `run_clean_test` with OS-aware allowlist:
```sh
run_clean_test() {
    env_args="PATH=$PATH"
    [ -n "${HOME:-}" ] && env_args="$env_args HOME=$HOME"
    [ -n "${GOROOT:-}" ] && env_args="$env_args GOROOT=$GOROOT"
    [ -n "${GOPATH:-}" ] && env_args="$env_args GOPATH=$GOPATH"
    [ -n "${GOCACHE:-}" ] && env_args="$env_args GOCACHE=$GOCACHE"

    # Windows / MSYS2 essential runtime allowlist
    [ -n "${SystemRoot:-}" ] && env_args="$env_args SystemRoot=$SystemRoot"
    [ -n "${SYSTEMROOT:-}" ] && env_args="$env_args SYSTEMROOT=$SYSTEMROOT"
    [ -n "${USERPROFILE:-}" ] && env_args="$env_args USERPROFILE=$USERPROFILE"
    [ -n "${TMP:-}" ] && env_args="$env_args TMP=$TMP"
    [ -n "${TEMP:-}" ] && env_args="$env_args TEMP=$TEMP"
    [ -n "${LOCALAPPDATA:-}" ] && env_args="$env_args LOCALAPPDATA=$LOCALAPPDATA"
    [ -n "${APPDATA:-}" ] && env_args="$env_args APPDATA=$APPDATA"
    [ -n "${COMSPEC:-}" ] && env_args="$env_args COMSPEC=$COMSPEC"
    [ -n "${PATHEXT:-}" ] && env_args="$env_args PATHEXT=$PATHEXT"

    # Unix / macOS TempDir resolution
    if [ -n "${TMPDIR:-}" ]; then
        env_args="$env_args TMPDIR=$TMPDIR"
    elif [ -d "/tmp" ]; then
        env_args="$env_args TMPDIR=/tmp"
    fi

    env_args="$env_args CGO_ENABLED=${CGO_ENABLED:-0}"
    env_args="$env_args GIT_CONFIG_NOSYSTEM=1"
    env_args="$env_args GIT_TERMINAL_PROMPT=0"

    env -i $env_args go test "$@"
}
```
Update `run_go_test()` to call `run_clean_test -v -p 1 ./...`.
4. In `scripts/check-coverage.sh`:
   - Use `run_clean_test -coverprofile="$prof_file" "$pkg"`.
   - Capture test logs: if tests fail, record a violation and output error logs rather than silently swallowing.

- [ ] **Step 3: Run static analysis & monorepo verification**

1. `git grep "os.Getenv" brain/pkg/ | grep -v "brain/pkg/config"` -> must return 0.
2. `git grep "config.GetEnv" brain/pkg/` -> must return 0.
3. `git grep "TEST_DATABASE_URL"` -> must return 0.
4. `git grep "GetDBPath"` -> must return 0.
5. Run: `sh scripts/verify.sh --full` under dirty ambient variables (`POSTGRES_HOST=postgres POSTGRES_PASSWORD=badpass GITHUB_PAT=fake`).
Expected: All checks pass 100%.
6. Run: `sh scripts/check-coverage.sh --check`
Expected: Monorepo statement coverage passes with all thresholds satisfied.

- [ ] **Step 4: Commit**

```bash
git add scripts/verify.sh scripts/check-coverage.sh scripts/migrate_sqlite_to_postgres.go .github/workflows/docker-publish.yml
git commit -m "feat(scripts): implement cross-platform clean room and excise TEST_DATABASE_URL"
```
