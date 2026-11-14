package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"

	_ "modernc.org/sqlite"
)

func TestServer_Endpoints(t *testing.T) {
	oldCounters, oldMeta := httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn
	defer func() {
		httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn = oldCounters, oldMeta
	}()

	store := newTestMemStore()
	_, _ = store.InsertFact(context.Background(), "Server fact test", testEmbedding(1.0))
	handler := NewToolHandler(&Config{}, store, &MockOllamaClient{embedding: testEmbedding(1.0)})
	server := NewServer(handler)
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	tests := []serverEndpointTestCase{
		{
			name:       "Health",
			method:     http.MethodGet,
			path:       "/health",
			wantStatus: http.StatusOK,
			wantSubstr: `{"status":"ok"}`,
		},
		{
			name:       "CoverageFlush",
			method:     http.MethodGet,
			path:       "/debug/coverage/flush",
			wantStatus: http.StatusOK,
		},
		{
			name:       "CoverageFlushError",
			method:     http.MethodGet,
			path:       "/debug/coverage/flush",
			counterErr: errors.New("flush err"),
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "FactsJSON",
			method:     http.MethodGet,
			path:       "/facts?limit=10&offset=0",
			wantStatus: http.StatusOK,
			wantSubstr: `"facts"`,
		},
		{
			name:       "FactsStoreError",
			method:     http.MethodGet,
			path:       "/facts",
			storeFail:  "GetFactsPaginated",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "FactsMethodNotAllowed",
			method:     http.MethodPost,
			path:       "/facts",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "ContextJSON",
			method:     http.MethodGet,
			path:       "/context?q=test&limit=5",
			wantStatus: http.StatusOK,
			wantSubstr: `"context"`,
		},
		{
			name:       "ContextPlaintext",
			method:     http.MethodGet,
			path:       "/context?q=test&limit=5",
			headers:    map[string]string{"Accept": "text/plain"},
			wantStatus: http.StatusOK,
			wantSubstr: "<retrieved_memory>",
		},
		{
			name:       "ContextMethodNotAllowed",
			method:     http.MethodPost,
			path:       "/context",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "ConsolidateOK",
			method:     http.MethodPost,
			path:       "/consolidate",
			wantStatus: http.StatusOK,
			wantSubstr: `"consolidated":0`,
		},
		{
			name:       "ConsolidateStoreError",
			method:     http.MethodPost,
			path:       "/consolidate",
			storeFail:  "GetAllFactsWithEmbeddings",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "ConsolidateMethodNotAllowed",
			method:     http.MethodGet,
			path:       "/consolidate",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "Metrics",
			method:     http.MethodGet,
			path:       "/metrics",
			wantStatus: http.StatusOK,
			wantSubstr: "coven_facts_total",
		},
		{
			name:       "MCPDirectMetricsRoute",
			method:     http.MethodGet,
			path:       "/metrics",
			directMCP:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MCPDirectConsolidateRoute",
			method:     http.MethodPost,
			path:       "/consolidate",
			directMCP:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MCPGetProbe",
			method:     http.MethodGet,
			path:       "/mcp",
			wantStatus: http.StatusOK,
			wantSubstr: "Aerial Memory MCP Server",
		},
		{
			name:       "MCPMethodNotAllowed",
			method:     http.MethodPut,
			path:       "/mcp",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "MCPEmptyBody",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "MCPReadError",
			method:     http.MethodPost,
			path:       "/mcp",
			directMCP:  true,
			errReader:  true,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "MCPDirectHealthRoute",
			method:     http.MethodGet,
			path:       "/health",
			directMCP:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MCPDirectFlushRoute",
			method:     http.MethodGet,
			path:       "/debug/coverage/flush",
			directMCP:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MCPDirectFactsRoute",
			method:     http.MethodGet,
			path:       "/facts",
			directMCP:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MCPDirectContextRoute",
			method:     http.MethodGet,
			path:       "/context?q=test",
			directMCP:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MCPDoublePrefixNormalized",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"memory_memory_search","arguments":{"query":"test"}}}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MCPStoreToolCall",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"memory_store","arguments":{"fact_text":"tool call store"}}}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MCPForgetToolCall",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"memory_forget","arguments":{"fact_id":1}}}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MCPToolsListDocumentsAdmissionGate",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       `{"jsonrpc":"2.0","id":4,"method":"tools/list","params":{}}`,
			wantStatus: http.StatusOK,
			wantSubstr: "admission gate constraints",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runServerEndpointCase(t, tc, store, server, ts)
		})
	}
}

type serverEndpointTestCase struct {
	name       string
	method     string
	path       string
	headers    map[string]string
	body       string
	counterErr error
	storeFail  string
	directMCP  bool
	errReader  bool
	wantStatus int
	wantSubstr string
}

func runServerEndpointCase(t *testing.T, tc serverEndpointTestCase, store *TestMemStore, server *Server, ts *httptest.Server) {
	t.Helper()
	httpkit.WriteCountersDirFn = func(string) error { return tc.counterErr }
	httpkit.WriteMetaDirFn = func(string) error { return tc.counterErr }
	if tc.storeFail != "" {
		store.SetFail(tc.storeFail, errors.New("store err"))
		defer store.SetFail("", nil)
	}

	if tc.directMCP {
		runServerDirectMCP(t, tc, server)
		return
	}

	runServerHTTP(t, tc, ts)
}

func runServerDirectMCP(t *testing.T, tc serverEndpointTestCase, server *Server) {
	t.Helper()
	var rBody io.Reader
	if tc.errReader {
		rBody = &mockErrReadCloser{}
	} else if tc.body != "" {
		rBody = strings.NewReader(tc.body)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(tc.method, tc.path, rBody)
	server.handleMCP(rec, req)
	if rec.Code != tc.wantStatus {
		t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
	}
}

func runServerHTTP(t *testing.T, tc serverEndpointTestCase, ts *httptest.Server) {
	t.Helper()
	var bodyReader io.Reader
	if tc.body != "" {
		bodyReader = strings.NewReader(tc.body)
	}
	req, err := http.NewRequest(tc.method, ts.URL+tc.path, bodyReader)
	if err != nil {
		t.Fatalf("http.NewRequest failed: %v", err)
	}
	for k, v := range tc.headers {
		req.Header.Set(k, v)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != tc.wantStatus {
		t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
	}
	if tc.wantSubstr != "" {
		respBytes, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(respBytes), tc.wantSubstr) {
			t.Errorf("response %q missing %q", string(respBytes), tc.wantSubstr)
		}
	}
}

func TestServer_PanicOnNilHandler(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected NewServer(nil) to panic")
		}
	}()
	_ = NewServer(nil)
}

func TestFactTime_Scan(t *testing.T) {
	now := time.Now().UTC()
	var nilTimePtr *time.Time

	tests := []struct {
		name      string
		input     any
		wantValid bool
		wantErr   bool
	}{
		{"NilInput", nil, false, false},
		{"NilTimePointer", nilTimePtr, false, false},
		{"ValidTimePointer", &now, true, false},
		{"ValidTimeValue", now, true, false},
		{"ValidRFC3339String", "2026-10-21T09:43:01Z", true, false},
		{"ValidSpaceString", "2026-10-21 09:00:00", true, false},
		{"EmptyString", "  ", false, false},
		{"InvalidDateString", "not-a-timestamp", false, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var ft FactTime
			err := ft.Scan(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("FactTime.Scan() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && ft.Valid != tc.wantValid {
				t.Errorf("ft.Valid = %v, want %v", ft.Valid, tc.wantValid)
			}
		})
	}
}

func TestEscapeSQLLikeAndHelpers(t *testing.T) {
	ctx, cancel := withTimeout(nil, 1*time.Second)
	defer cancel()
	if ctx == nil {
		t.Fatal("expected non-nil context from withTimeout(nil)")
	}

	if got := EscapeSQLLike("a%b_c\\d"); got != "a\\%b\\_c\\\\d" {
		t.Errorf("EscapeSQLLike() = %q", got)
	}
}

func TestOpenDB(t *testing.T) {
	addr := startMockPostgres(t)
	validDSN := "postgres://mock_user:mock_pass@" + addr + "/mock_db?sslmode=disable"

	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{"NilConfig", nil, true},
		{"EmptyDSN", &Config{DatabaseURL: "   "}, true},
		{"UnreachableDSN", &Config{DatabaseURL: "postgres://127.0.0.1:1/bad?connect_timeout=1"}, true},
		{"ValidMockPostgres", &Config{DatabaseURL: validDSN}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := OpenDB(tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("OpenDB() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if db != nil {
				_ = db.Close()
			}
		})
	}
}

func TestMain_Execution(t *testing.T) {
	oldRun, oldExit, oldPath := runAppFn, exitFn, configPath
	defer func() {
		runAppFn, exitFn, configPath = oldRun, oldExit, oldPath
		installTestTracing() // main() runs tracing.Init, which replaces the global provider.
	}()

	tmpDir := t.TempDir()
	testCfg := filepath.Join(tmpDir, "config.yaml")
	testCfgContent := `port: "4008"
database_url: "postgres://u:p@localhost/db"
eri_url: "http://eri.aerial"
eri_llm_url: "http://eri.aerial/internal/llm"
pr_register_url: "http://synapse.aerial/internal/pr/register"
custom_skills_dir: "/config/custom-skills"
rules_dir: "/config/rules"
config_repo_owner: "azylman"
config_repo_name: "aerial-config"
ollama:
  base_url: "http://ollama:11434"
  model: "all-minilm"
  num_ctx: 512
`
	_ = os.WriteFile(testCfg, []byte(testCfgContent), 0o644)

	tests := []struct {
		name         string
		cfgPath      string
		dbURL        string
		runErr       error
		wantRunCall  bool
		wantExitCall bool
	}{
		{
			name:        "CleanExit",
			cfgPath:     testCfg,
			dbURL:       "postgres://user:pass@localhost:5432/db",
			wantRunCall: true,
		},
		{
			name:        "ErrServerClosedIgnored",
			cfgPath:     testCfg,
			dbURL:       "postgres://user:pass@localhost:5432/db",
			runErr:      http.ErrServerClosed,
			wantRunCall: true,
		},
		{
			name:         "RuntimeFailureCallsExit",
			cfgPath:      testCfg,
			dbURL:        "postgres://user:pass@localhost:5432/db",
			runErr:       context.DeadlineExceeded,
			wantRunCall:  true,
			wantExitCall: true,
		},
		{
			name:         "ConfigLoadFailureCallsExit",
			cfgPath:      filepath.Join(tmpDir, "nonexistent.yaml"),
			dbURL:        "",
			wantExitCall: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configPath = tc.cfgPath
			t.Setenv("CONFIG_PATH", tc.cfgPath)
			t.Setenv("DATABASE_URL", tc.dbURL)

			runCalled := false
			exitCalled := false
			runAppFn = func(ctx context.Context, cfg *Config) error {
				runCalled = true
				return tc.runErr
			}
			exitFn = func(string, ...any) {
				exitCalled = true
			}

			main()
			if runCalled != tc.wantRunCall {
				t.Errorf("runCalled = %v, want %v", runCalled, tc.wantRunCall)
			}
			if exitCalled != tc.wantExitCall {
				t.Errorf("exitCalled = %v, want %v", exitCalled, tc.wantExitCall)
			}
		})
	}
}

func TestRunApp_Lifecycle(t *testing.T) {
	addr := startMockPostgres(t)
	validDSN := "postgres://mock_user:mock_pass@" + addr + "/mock_db?sslmode=disable"

	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name:    "NilConfigFails",
			cfg:     nil,
			wantErr: true,
		},
		{
			name:    "InvalidDBFails",
			cfg:     &Config{DatabaseURL: "postgres://127.0.0.1:1/bad?connect_timeout=1"},
			wantErr: true,
		},
		{
			name:    "CleanLifecycleWithMockPostgres",
			cfg:     &Config{Port: "0", DatabaseURL: validDSN},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := RunApp(ctx, tc.cfg)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Fatalf("RunApp() unexpected err = %v", err)
			}
		})
	}
}

func TestConfig_LoadAndResolve_SkillsAndPRRegistryEnvOverrides(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "chunk4.yaml")
	content := `port: "4008"
database_url: "postgres://localhost/test"
eri_url: "http://eri.aerial"
eri_llm_url: "http://eri.aerial/internal/llm"
custom_skills_dir: "/yaml/custom-skills"
rules_dir: "/yaml/rules"
github_pat: "pat-from-yaml"
config_repo_owner: "yaml-owner"
config_repo_name: "yaml-repo"
pr_register_url: "http://yaml-brain/internal/pr/register"
system_channel_id: "1562837549255495693"
ollama:
  base_url: "http://ollama:11434"
  model: "all-minilm"
  num_ctx: 512
`
	if err := os.WriteFile(yamlPath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write yaml: %v", err)
	}

	tests := []struct {
		name          string
		path          string
		envSkillsDir  string
		envPAT        string
		envOwner      string
		envRepo       string
		envBrainReg   string
		envChannelID  string
		wantErr       bool
		wantSkillsDir string
		wantPAT       string
		wantOwner     string
		wantRepo      string
		wantBrainReg  string
		wantChannelID string
	}{
		{
			name:          "LoadedFromYAML",
			path:          yamlPath,
			wantSkillsDir: "/yaml/custom-skills",
			wantPAT:       "pat-from-yaml",
			wantOwner:     "yaml-owner",
			wantRepo:      "yaml-repo",
			wantBrainReg:  "http://yaml-brain/internal/pr/register",
			wantChannelID: "1562837549255495693",
		},
		{
			name:          "SecretsEnvOverridesYAML_NonSecretsIgnored",
			path:          yamlPath,
			envSkillsDir:  "/env/custom-skills",
			envPAT:        "pat-from-env",
			envOwner:      "env-owner",
			envRepo:       "env-repo",
			envBrainReg:   "http://env-brain/pr/reg",
			envChannelID:  "1562837549255495694",
			wantSkillsDir: "/yaml/custom-skills",
			wantPAT:       "pat-from-env",
			wantOwner:     "yaml-owner",
			wantRepo:      "yaml-repo",
			wantBrainReg:  "http://yaml-brain/internal/pr/register",
			wantChannelID: "1562837549255495694",
		},
		{
			name:          "NonNumericChannelIDCleared",
			path:          yamlPath,
			envChannelID:  "general-channel-name",
			wantSkillsDir: "/yaml/custom-skills",
			wantPAT:       "pat-from-yaml",
			wantOwner:     "yaml-owner",
			wantRepo:      "yaml-repo",
			wantBrainReg:  "http://yaml-brain/internal/pr/register",
			wantChannelID: "",
		},
		{
			name:    "EmptyPathFails",
			path:    "",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://localhost/test")
			t.Setenv("ERI_URL", "")
			t.Setenv("ERI_LLM_URL", "")
			t.Setenv("CUSTOM_SKILLS_DIR", tc.envSkillsDir)
			t.Setenv("GITHUB_PAT", tc.envPAT)
			t.Setenv("CONFIG_REPO_OWNER", tc.envOwner)
			t.Setenv("CONFIG_REPO_NAME", tc.envRepo)
			t.Setenv("BRAIN_PR_REGISTER_URL", tc.envBrainReg)
			t.Setenv("SYSTEM_CHANNEL_ID", tc.envChannelID)

			cfg, err := LoadConfigFile(tc.path, "")
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfigFile() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if cfg.CustomSkillsDir != tc.wantSkillsDir {
				t.Errorf("CustomSkillsDir = %q, want %q", cfg.CustomSkillsDir, tc.wantSkillsDir)
			}
			if cfg.GitHubPAT != tc.wantPAT {
				t.Errorf("GitHubPAT = %q, want %q", cfg.GitHubPAT, tc.wantPAT)
			}
			if cfg.ConfigRepoOwner != tc.wantOwner {
				t.Errorf("ConfigRepoOwner = %q, want %q", cfg.ConfigRepoOwner, tc.wantOwner)
			}
			if cfg.ConfigRepoName != tc.wantRepo {
				t.Errorf("ConfigRepoName = %q, want %q", cfg.ConfigRepoName, tc.wantRepo)
			}
			if cfg.BrainPRRegisterURL != tc.wantBrainReg {
				t.Errorf("BrainPRRegisterURL = %q, want %q", cfg.BrainPRRegisterURL, tc.wantBrainReg)
			}
			if cfg.SystemChannelID != tc.wantChannelID {
				t.Errorf("SystemChannelID = %q, want %q", cfg.SystemChannelID, tc.wantChannelID)
			}
		})
	}
}

func TestConfig_BrainURLResolution(t *testing.T) {
	tmpDir := t.TempDir()
	baseContent := `port: "4008"
database_url: "postgres://localhost/test"
eri_url: "http://custom-eri:8000"
eri_llm_url: "http://custom-proxy:9000/v1/llm"
pr_register_url: "http://synapse.aerial/internal/pr/register"
custom_skills_dir: "/config/custom-skills"
rules_dir: "/config/rules"
config_repo_owner: "azylman"
config_repo_name: "aerial-config"
ollama:
  base_url: "http://ollama:11434"
  model: "all-minilm"
  num_ctx: 512
`
	validFile := filepath.Join(tmpDir, "valid.yaml")
	_ = os.WriteFile(validFile, []byte(baseContent), 0o644)

	noBrainFile := filepath.Join(tmpDir, "no_brain.yaml")
	_ = os.WriteFile(noBrainFile, []byte(strings.Replace(baseContent, `eri_url: "http://custom-eri:8000"`, `eri_url: ""`, 1)), 0o644)

	noLLMFile := filepath.Join(tmpDir, "no_llm.yaml")
	_ = os.WriteFile(noLLMFile, []byte(strings.Replace(baseContent, `eri_llm_url: "http://custom-proxy:9000/v1/llm"`, `eri_llm_url: ""`, 1)), 0o644)

	noPRRegFile := filepath.Join(tmpDir, "no_pr_reg.yaml")
	_ = os.WriteFile(noPRRegFile, []byte(strings.Replace(baseContent, `pr_register_url: "http://synapse.aerial/internal/pr/register"`, `pr_register_url: ""`, 1)), 0o644)

	tests := []struct {
		name         string
		path         string
		wantErr      bool
		wantBrainURL string
		wantBrainLLM string
	}{
		{
			name:         "ValidBrainURLs",
			path:         validFile,
			wantBrainURL: "http://custom-eri:8000",
			wantBrainLLM: "http://custom-proxy:9000/v1/llm",
		},
		{
			name:    "MissingBrainURLFails",
			path:    noBrainFile,
			wantErr: true,
		},
		{
			name:    "MissingBrainLLMURLFails",
			path:    noLLMFile,
			wantErr: true,
		},
		{
			name:    "MissingBrainPRRegisterURLFails",
			path:    noPRRegFile,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ERI_URL", "")
			t.Setenv("ERI_LLM_URL", "")
			t.Setenv("PR_REGISTER_URL", "")
			cfg, err := LoadConfigFile(tc.path, "")
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfigFile() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if cfg.BrainURL != tc.wantBrainURL {
				t.Errorf("BrainURL = %q, want %q", cfg.BrainURL, tc.wantBrainURL)
			}
			if cfg.BrainLLMURL != tc.wantBrainLLM {
				t.Errorf("BrainLLMURL = %q, want %q", cfg.BrainLLMURL, tc.wantBrainLLM)
			}
		})
	}
}

func TestLoadConfig_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()
	validContent := `port: '4011'
database_url: 'postgres://localhost/coven_test'
eri_url: 'http://eri.aerial'
eri_llm_url: 'http://eri.aerial/internal/llm'
pr_register_url: 'http://synapse.aerial/internal/pr/register'
custom_skills_dir: '/custom/skills'
rules_dir: '/custom/rules'
config_repo_owner: 'azylman'
config_repo_name: 'aerial-config'
ollama:
  base_url: 'http://ollama:11434'
  model: 'all-minilm'
  num_ctx: 512
`
	validFile := filepath.Join(tmpDir, "config.yaml")
	_ = os.WriteFile(validFile, []byte(validContent), 0o644)

	noDbContent := strings.Replace(validContent, "database_url: 'postgres://localhost/coven_test'", "database_url: ''", 1)
	noDbFile := filepath.Join(tmpDir, "nodb.yaml")
	_ = os.WriteFile(noDbFile, []byte(noDbContent), 0o644)

	mkMissingFieldFile := func(target string, replacement string, filename string) string {
		p := filepath.Join(tmpDir, filename)
		_ = os.WriteFile(p, []byte(strings.Replace(validContent, target, replacement, 1)), 0o644)
		return p
	}

	tests := []struct {
		name         string
		path         string
		envDB        string
		envPassword  string
		wantPort     string
		wantRulesDir string
		wantDB       string
		wantErr      bool
	}{
		{
			name:         "valid file and env database url",
			path:         validFile,
			envDB:        "postgres://localhost/coven_test",
			wantPort:     "4011",
			wantRulesDir: "/custom/rules",
			wantErr:      false,
		},
		{
			name:         "valid file with file database url",
			path:         validFile,
			envDB:        "",
			wantPort:     "4011",
			wantRulesDir: "/custom/rules",
			wantErr:      false,
		},
		{
			name:         "valid file with decomposed postgres env vars",
			path:         noDbFile,
			envPassword:  "secretpass",
			wantPort:     "4011",
			wantRulesDir: "/custom/rules",
			wantDB:       "postgres://aerial:secretpass@postgres:5432/aerial?sslmode=disable",
			wantErr:      false,
		},
		{
			name:    "missing database url returns error",
			path:    noDbFile,
			envDB:   "",
			wantErr: true,
		},
		{
			name:    "empty path returns error",
			path:    "",
			envDB:   "postgres://localhost/coven_test",
			wantErr: true,
		},
		{
			name:    "nonexistent path returns error",
			path:    filepath.Join(tmpDir, "nonexistent.yaml"),
			envDB:   "postgres://localhost/coven_test",
			wantErr: true,
		},
		{
			name:    "missing custom_skills_dir returns error",
			path:    mkMissingFieldFile("custom_skills_dir: '/custom/skills'", "custom_skills_dir: ''", "no_skills.yaml"),
			wantErr: true,
		},
		{
			name:    "missing rules_dir returns error",
			path:    mkMissingFieldFile("rules_dir: '/custom/rules'", "rules_dir: ''", "no_rules.yaml"),
			wantErr: true,
		},
		{
			name:    "missing config_repo_owner returns error",
			path:    mkMissingFieldFile("config_repo_owner: 'azylman'", "config_repo_owner: ''", "no_owner.yaml"),
			wantErr: true,
		},
		{
			name:    "missing config_repo_name returns error",
			path:    mkMissingFieldFile("config_repo_name: 'aerial-config'", "config_repo_name: ''", "no_name.yaml"),
			wantErr: true,
		},
		{
			name:    "missing ollama base_url returns error",
			path:    mkMissingFieldFile("base_url: 'http://ollama:11434'", "base_url: ''", "no_ollama_url.yaml"),
			wantErr: true,
		},
		{
			name:    "missing ollama model returns error",
			path:    mkMissingFieldFile("model: 'all-minilm'", "model: ''", "no_ollama_model.yaml"),
			wantErr: true,
		},
		{
			name:    "zero ollama num_ctx returns error",
			path:    mkMissingFieldFile("num_ctx: 512", "num_ctx: 0", "zero_num_ctx.yaml"),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", tc.envDB)
			t.Setenv("POSTGRES_URL", "")
			t.Setenv("POSTGRES_HOST", "")
			t.Setenv("POSTGRES_PORT", "")
			t.Setenv("POSTGRES_USER", "")
			t.Setenv("POSTGRES_PASSWORD", tc.envPassword)
			t.Setenv("POSTGRES_DB", "")
			cfg, err := LoadConfig(tc.path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadConfig() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if cfg.Port != tc.wantPort {
				t.Errorf("Port = %q, want %q", cfg.Port, tc.wantPort)
			}
			if cfg.RulesDir != tc.wantRulesDir {
				t.Errorf("RulesDir = %q, want %q", cfg.RulesDir, tc.wantRulesDir)
			}
			if tc.wantDB != "" && cfg.DatabaseURL != tc.wantDB {
				t.Errorf("DatabaseURL = %q, want %q", cfg.DatabaseURL, tc.wantDB)
			}
		})
	}
}

func TestFormatSkillCommitAndPRHelpers(t *testing.T) {
	c1 := SkillFileChange{
		RelPath:       "custom-skills/discord/ha/SKILL.md",
		CommitSummary: "update ha",
		Action:        "EDIT",
	}
	c2 := SkillFileChange{
		RelPath:       "custom-skills/common/deploy/SKILL.md",
		CommitSummary: "add deploy",
		Action:        "CREATE",
	}

	tests := []struct {
		name          string
		changes       []SkillFileChange
		targetID      string
		wantCommitSub string
		wantTitle     string
		wantBodySub   string
	}{
		{
			name:          "SingleChange",
			changes:       []SkillFileChange{c1},
			targetID:      "1562837549255495693",
			wantCommitSub: "feat(skills): update ha",
			wantTitle:     "feat(skills): update ha",
			wantBodySub:   `<!-- aerial-pr: {"target_id":"1562837549255495693","vanadis_chunk_id":""} -->`,
		},
		{
			name:          "MultipleChanges",
			changes:       []SkillFileChange{c1, c2},
			targetID:      "",
			wantCommitSub: "feat(skills): update 2 procedural skills",
			wantTitle:     "feat(skills): evolve 2 procedural skills",
			wantBodySub:   `<!-- aerial-pr: {"target_id":"","vanadis_chunk_id":""} -->`,
		},
		{
			name: "RequireReviewChangeClearsTargetIDAndRendersManualReviewBanner",
			changes: []SkillFileChange{{
				RelPath:       "custom-skills/discord/ha/SKILL.md",
				CommitSummary: "update ha",
				Action:        "EDIT",
				RequireReview: true,
				CandidateIDs:  []int64{101},
				Candidates: []ProceduralCandidate{{
					ID:          101,
					LoadedSkill: "self-improvement",
					Trigger:     "when running recursive grep",
					Steps:       []string{"pass --exclude-dir=.git"},
					Avoid:       []string{"do not run bare grep -r"},
				}},
			}},
			targetID:      "1562837549255495693",
			wantCommitSub: "feat(skills): update ha",
			wantTitle:     "feat(skills): update ha",
			wantBodySub:   `<!-- aerial-pr: {"target_id":"","vanadis_chunk_id":""} -->`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			commitMsg := formatSkillCommitMessage(tc.changes)
			if !strings.Contains(commitMsg, tc.wantCommitSub) {
				t.Errorf("commitMsg = %q, want containing %q", commitMsg, tc.wantCommitSub)
			}
			title := formatSkillPRTitle(tc.changes)
			if title != tc.wantTitle {
				t.Errorf("title = %q, want %q", title, tc.wantTitle)
			}
			body := formatSkillPRBody(tc.changes, tc.targetID)
			if !strings.Contains(body, tc.wantBodySub) {
				t.Errorf("body = %q, want containing %q", body, tc.wantBodySub)
			}
		})
	}

	longTrigger := strings.Repeat("x", 1800)
	alertChanges := []SkillFileChange{{
		RelPath:       "custom-skills/discord/ha/SKILL.md",
		CommitSummary: "update ha",
		Action:        "EDIT",
		RequireReview: true,
		CandidateIDs:  []int64{101},
		Candidates: []ProceduralCandidate{{
			ID:          101,
			LoadedSkill: "self-improvement",
			Trigger:     longTrigger,
			Steps:       []string{"step 1", "step 2"},
			Avoid:       []string{"avoid 1"},
		}},
	}}
	alertMsg := formatManualReviewPRAlert(42, "azylman", "aerial-config", alertChanges)
	if len(alertMsg) > 1800 {
		t.Errorf("formatManualReviewPRAlert len = %d, want <= 1800", len(alertMsg))
	}
	if !strings.Contains(alertMsg, "PR [#42](https://github.com/azylman/aerial-config/pull/42)") {
		t.Errorf("formatManualReviewPRAlert missing PR link: %q", alertMsg)
	}

	// Test parseDesignerResponse
	parseTests := []struct {
		name       string
		raw        string
		wantErr    bool
		wantAction string
	}{
		{name: "EmptyRaw", raw: "", wantErr: true},
		{name: "NoJSON", raw: "hello world", wantErr: true},
		{name: "MalformedJSON", raw: "{broken json", wantErr: true},
		{name: "ValidJSON", raw: `{"action":"EDIT","name":"foo"}`, wantErr: false, wantAction: "EDIT"},
		{name: "ValidJSONWithMarkdownPrefix", raw: "```json\n{\"action\":\"CREATE\",\"name\":\"bar\"}\n```", wantErr: false, wantAction: "CREATE"},
	}

	for _, tc := range parseTests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := parseDesignerResponse(tc.raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseDesignerResponse(%q) err = %v, wantErr = %v", tc.raw, err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if resp.Action != tc.wantAction {
				t.Errorf("Action = %q, want %q", resp.Action, tc.wantAction)
			}
		})
	}
}

func TestServer_Metrics_Table(t *testing.T) {
	store := newTestMemStore()
	_, _ = store.InsertFact(context.Background(), "Metric fact 1", testEmbedding(1.0))
	_, _ = store.InsertFact(context.Background(), "Metric fact 2", testEmbedding(1.0))
	handler := NewToolHandler(&Config{}, store, &MockOllamaClient{embedding: testEmbedding(1.0)})
	server := NewServer(handler)
	nilServer := &Server{}
	nilStoreServer := &Server{handler: &ToolHandler{}}

	registerCovenCollector(FactsTotal)

	RecordOperation("search", "success", 0.01)
	RecordOperation("search", "error", 0.02)
	RecordOperation("store", "success", 0.03)
	RecordOperation("store", "error", 0.04)
	RecordOperation("forget", "success", 0.05)
	RecordOperation("context", "success", 0.06)
	RecordOperation("extract", "success", 0.07)
	RecordOperation("consolidate", "success", 0.08)

	RecordFactStored("inserted")
	RecordFactStored("updated")
	RecordFactStored("reinforced")
	RecordFactStored("deleted")

	tests := []struct {
		name           string
		server         *Server
		method         string
		url            string
		wantStatus     int
		wantSubstrings []string
	}{
		{
			name:       "valid metrics with facts total and operations",
			server:     server,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"coven_facts_total 2",
				"coven_operations_total",
				`coven_operations_total{op="search",status="success"}`,
				`coven_operations_total{op="search",status="error"}`,
				`coven_operations_total{op="store",status="success"}`,
				`coven_operations_total{op="store",status="error"}`,
				`coven_operations_total{op="forget",status="success"}`,
				`coven_operations_total{op="context",status="success"}`,
				`coven_operations_total{op="extract",status="success"}`,
				`coven_operations_total{op="consolidate",status="success"}`,
				"coven_operation_duration_seconds",
				"coven_facts_stored_total",
				`coven_facts_stored_total{action="inserted"}`,
				`coven_facts_stored_total{action="updated"}`,
				`coven_facts_stored_total{action="reinforced"}`,
				`coven_facts_stored_total{action="deleted"}`,
			},
		},
		{
			name:       "nil server still serves metrics ok",
			server:     nilServer,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"coven_facts_total",
				"coven_operations_total",
			},
		},
		{
			name:       "nil store server serves metrics ok",
			server:     nilStoreServer,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"coven_facts_total",
				"coven_operations_total",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.url, nil)
			tc.server.handleMetrics(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("handleMetrics() status = %d, want %d", rec.Code, tc.wantStatus)
			}
			body := rec.Body.String()
			for _, sub := range tc.wantSubstrings {
				if !strings.Contains(body, sub) {
					t.Errorf("expected body to contain %q, body:\n%s", sub, body)
				}
			}
		})
	}
}
