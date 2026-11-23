package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// covenDarkBaseConfig is a complete, valid coven config; rows append keys or swap values on it.
const covenDarkBaseConfig = `port: '4011'
database_url: 'postgres://localhost/coven_test'
eri_url: 'http://eri.aerial'
eri_llm_url: 'http://eri.aerial/internal/llm'
pr_register_url: 'http://synapse.aerial/internal/pr/register'
custom_skills_dir: '/custom/skills'
config_repo_owner: 'example-owner'
config_repo_name: 'example-config'
ollama:
  base_url: 'http://senses.aerial:8080'
  model: 'bge-small-en-v1.5'
  num_ctx: 512
`

// covenExtractorConfig is the minimal extractor-role config: no PR or config repo keys.
const covenExtractorConfig = `port: '4011'
database_url: 'postgres://localhost/coven_test'
eri_url: 'http://eri.aerial'
eri_llm_url: 'http://eri.aerial/internal/llm'
role: 'extractor'
stack: 'agent-alpha'
ollama:
  base_url: 'http://senses.aerial:8080'
  model: 'bge-small-en-v1.5'
  num_ctx: 512
`

// covenConfigSwap returns base with each old/new pair replaced.
func covenConfigSwap(base string, oldNew ...string) string {
	return strings.NewReplacer(oldNew...).Replace(base)
}

// covenConfigFields renders every asserted Config field (and derived accessor) by name.
func covenConfigFields(cfg *Config) map[string]string {
	return map[string]string{
		"Port":                cfg.Port,
		"RestatePort":         cfg.RestatePort,
		"RestateIngressURL":   cfg.RestateIngressURL,
		"RestateAdminURL":     cfg.RestateAdminURL,
		"RestateAdvertiseURL": cfg.RestateAdvertiseURL,
		"DatabaseURL":         cfg.DatabaseURL,
		"EriURL":              cfg.EriURL,
		"EriLLMURL":           cfg.EriLLMURL,
		"PRRegisterURL":       cfg.PRRegisterURL,
		"CustomSkillsDir":     cfg.CustomSkillsDir,
		"GitHubPAT":           cfg.GitHubPAT,
		"ConfigRepoOwner":     cfg.ConfigRepoOwner,
		"ConfigRepoName":      cfg.ConfigRepoName,
		"SystemChannelID":     cfg.SystemChannelID,
		"Role":                cfg.Role,
		"TranscriptsURL":      cfg.TranscriptsURL,
		"EffectiveStack":      cfg.EffectiveStack(),
		"ExtractTimeout":      cfg.ExtractTimeout().String(),
		"ExtractBatchLimit":   strconv.Itoa(cfg.ExtractBatchLimit()),
		"Auth.Enabled":        strconv.FormatBool(cfg.Auth.Enabled),
	}
}

// covenLoadConfig writes yaml to a temp file (or resolves path under a temp dir when yaml is empty)
// and loads it through LoadConfigFile when dbArg is set, else through LoadConfig.
func covenLoadConfig(t *testing.T, yaml, path, dbArg string, env map[string]string) (*Config, error) {
	t.Helper()
	if yaml != "" {
		path = filepath.Join(t.TempDir(), "coven.yaml")
		if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
			t.Fatalf("write config: %v", err)
		}
	} else if path != "" {
		path = filepath.Join(t.TempDir(), path)
	}
	if dbArg != "" {
		return LoadConfigFile(path, dbArg, mapEnv(env))
	}
	return LoadConfig(path, mapEnv(env))
}

// covenAssertFields checks each named field in want against covenConfigFields(cfg).
func covenAssertFields(t *testing.T, cfg *Config, want map[string]string) {
	t.Helper()
	got := covenConfigFields(cfg)
	for field, wantVal := range want {
		gotVal, ok := got[field]
		if !ok {
			t.Fatalf("unknown want field %q", field)
		}
		if gotVal != wantVal {
			t.Errorf("%s = %q, want %q", field, gotVal, wantVal)
		}
	}
}

// covenTestDSN is the database URL passed to LoadConfigFile by rows that set dbArg.
const covenTestDSN = "postgres://localhost/coven_test"

// covenLoadConfigCases drives TestLoadConfigFile: LoadConfigFile when dbArg is set, else LoadConfig.
// yaml empty means no file is written and path is used as given (joined under a temp dir when set).
// want lists the fields to check by covenConfigFields name; wantErr is a literal error substring.
var covenLoadConfigCases = []struct {
	name    string
	yaml    string
	path    string
	dbArg   string
	env     map[string]string
	wantErr string
	want    map[string]string
}{
	{
		name:  "empty stack defaults to aerial with transcripts, timeout and auth defaults",
		yaml:  covenDarkBaseConfig,
		dbArg: covenTestDSN,
		want: map[string]string{
			"EffectiveStack":      "aerial",
			"Role":                "",
			"TranscriptsURL":      "http://transcripts.aerial",
			"ExtractTimeout":      "2m0s",
			"ExtractBatchLimit":   "50",
			"Auth.Enabled":        "false",
			"RestatePort":         "9080",
			"RestateIngressURL":   "http://restate.aerial:8080",
			"RestateAdminURL":     "http://restate.aerial:9070",
			"RestateAdvertiseURL": "http://coven.default.svc.cluster.local:9080",
		},
	},
	{
		name:  "restate config loads from yaml without env",
		yaml:  covenDarkBaseConfig + "restate_port: '9081'\nrestate_ingress_url: 'http://yaml.restate:8080'\nrestate_admin_url: 'http://yaml.restate:9070'\nrestate_advertise_url: 'http://yaml.coven:9080'\n",
		dbArg: covenTestDSN,
		want: map[string]string{
			"RestatePort":         "9081",
			"RestateIngressURL":   "http://yaml.restate:8080",
			"RestateAdminURL":     "http://yaml.restate:9070",
			"RestateAdvertiseURL": "http://yaml.coven:9080",
		},
	},
	{
		name:  "restate config loads from yaml and env overrides",
		yaml:  covenDarkBaseConfig + "restate_port: '9081'\nrestate_ingress_url: 'http://yaml.restate:8080'\nrestate_admin_url: 'http://yaml.restate:9070'\nrestate_advertise_url: 'http://yaml.coven:9080'\n",
		dbArg: covenTestDSN,
		env: map[string]string{
			"RESTATE_PORT":          "9082",
			"RESTATE_INGRESS_URL":   "http://env.restate:8080",
			"RESTATE_ADMIN_URL":     "http://env.restate:9070",
			"RESTATE_ADVERTISE_URL": "http://env.coven:9080",
		},
		want: map[string]string{
			"RestatePort":         "9082",
			"RestateIngressURL":   "http://env.restate:8080",
			"RestateAdminURL":     "http://env.restate:9070",
			"RestateAdvertiseURL": "http://env.coven:9080",
		},
	},
	{
		name:  "explicit stack fields load",
		dbArg: covenTestDSN,
		yaml: covenDarkBaseConfig + "stack: 'canary'\nrole: 'coven_aerial'\ntranscripts_url: 'http://indexer:4020'\n" +
			"extract_timeout_seconds: 30\nextract_max_steps: 25\n",
		want: map[string]string{
			"EffectiveStack":    "canary",
			"Role":              "coven_aerial",
			"TranscriptsURL":    "http://indexer:4020",
			"ExtractTimeout":    "30s",
			"ExtractBatchLimit": "25",
		},
	},
	{
		name:  "indexer_url acts as alias for transcripts_url",
		yaml:  covenDarkBaseConfig + "stack: 'canary'\nindexer_url: 'http://indexer-alias:4020'\n",
		dbArg: covenTestDSN,
		want: map[string]string{
			"EffectiveStack":    "canary",
			"Role":              "",
			"TranscriptsURL":    "http://indexer-alias:4020",
			"ExtractTimeout":    "2m0s",
			"ExtractBatchLimit": "50",
		},
	},
	{
		name:  "non-aerial stack with empty transcripts_url does not default",
		yaml:  covenDarkBaseConfig + "stack: 'canary'\n",
		dbArg: covenTestDSN,
		want: map[string]string{
			"EffectiveStack":    "canary",
			"Role":              "",
			"TranscriptsURL":    "",
			"ExtractTimeout":    "2m0s",
			"ExtractBatchLimit": "50",
		},
	},
	{
		name:  "positive extract env overrides yaml",
		yaml:  covenDarkBaseConfig + "extract_timeout_seconds: 30\nextract_max_steps: 25\n",
		dbArg: covenTestDSN,
		env:   map[string]string{"EXTRACT_TIMEOUT_SECONDS": "45", "EXTRACT_MAX_STEPS": "10"},
		want:  map[string]string{"ExtractTimeout": "45s", "ExtractBatchLimit": "10"},
	},
	{
		name:  "invalid extract env falls back to yaml",
		yaml:  covenDarkBaseConfig + "extract_timeout_seconds: 30\nextract_max_steps: 25\n",
		dbArg: covenTestDSN,
		env:   map[string]string{"EXTRACT_TIMEOUT_SECONDS": "not-a-number", "EXTRACT_MAX_STEPS": "0"},
		want:  map[string]string{"ExtractTimeout": "30s", "ExtractBatchLimit": "25"},
	},
	{
		name:  "unset extract env keeps yaml values",
		yaml:  covenDarkBaseConfig + "extract_timeout_seconds: 30\nextract_max_steps: 25\n",
		dbArg: covenTestDSN,
		want:  map[string]string{"ExtractTimeout": "30s", "ExtractBatchLimit": "25"},
	},
	{
		name:  "extractor role succeeds without pr and config repo keys",
		yaml:  covenExtractorConfig,
		dbArg: covenTestDSN,
		want:  map[string]string{"Role": "extractor"},
	},
	{
		name:    "all role requires pr_register_url",
		yaml:    covenConfigSwap(covenExtractorConfig, "role: 'extractor'", "role: 'all'", "stack: 'agent-alpha'", "stack: 'aerial'"),
		dbArg:   covenTestDSN,
		wantErr: "config: pr_register_url is required in ",
	},
	{name: "malformed yaml returns error", yaml: "port: [bad yaml", dbArg: covenTestDSN, wantErr: "config: failed to parse YAML from "},
	{
		name:    "missing port returns error",
		yaml:    covenConfigSwap(covenExtractorConfig, "port: '4011'", "port: ''"),
		dbArg:   covenTestDSN,
		wantErr: "config: port is required in ",
	},
	{
		name:  "auth enabled from yaml",
		yaml:  covenDarkBaseConfig + "auth:\n  enabled: true\n",
		dbArg: covenTestDSN,
		want:  map[string]string{"Auth.Enabled": "true"},
	},
	{
		name:  "auth env enables over yaml",
		yaml:  covenDarkBaseConfig + "auth:\n  enabled: false\n",
		dbArg: covenTestDSN,
		env:   map[string]string{"K8S_AUTH_ENABLED": "true"},
		want:  map[string]string{"Auth.Enabled": "true"},
	},
	{
		name:  "auth env disables over yaml",
		yaml:  covenDarkBaseConfig + "auth:\n  enabled: true\n",
		dbArg: covenTestDSN,
		env:   map[string]string{"K8S_AUTH_ENABLED": "false"},
		want:  map[string]string{"Auth.Enabled": "false"},
	},
	{
		name:    "invalid auth env fails config load",
		yaml:    covenDarkBaseConfig + "auth:\n  enabled: true\n",
		dbArg:   covenTestDSN,
		env:     map[string]string{"K8S_AUTH_ENABLED": "yes-please"},
		wantErr: `config: invalid K8S_AUTH_ENABLED "yes-please"`,
	},
	{
		name: "yaml fields load and env database url wins",
		yaml: covenDarkBaseConfig,
		env:  map[string]string{"DATABASE_URL": "postgres://localhost/testdb"},
		want: map[string]string{
			"Port":          "4011",
			"DatabaseURL":   "postgres://localhost/testdb",
			"EriURL":        "http://eri.aerial",
			"EriLLMURL":     "http://eri.aerial/internal/llm",
			"PRRegisterURL": "http://synapse.aerial/internal/pr/register",
		},
	},
	{
		name: "yaml database url used when env has none",
		yaml: covenDarkBaseConfig,
		want: map[string]string{"Port": "4011", "DatabaseURL": "postgres://localhost/coven_test"},
	},
	{
		name: "custom eri urls load from yaml",
		yaml: covenConfigSwap(covenDarkBaseConfig,
			"eri_url: 'http://eri.aerial'", "eri_url: 'http://custom-eri:8000'",
			"eri_llm_url: 'http://eri.aerial/internal/llm'", "eri_llm_url: 'http://custom-proxy:9000/v1/llm'"),
		want: map[string]string{"EriURL": "http://custom-eri:8000", "EriLLMURL": "http://custom-proxy:9000/v1/llm"},
	},
	{
		name: "eri and pr register env override yaml",
		yaml: covenDarkBaseConfig,
		env: map[string]string{
			"ERI_URL":         "http://env.eri:8088",
			"ERI_LLM_URL":     "http://env.eri:8088/internal/llm",
			"PR_REGISTER_URL": "http://env.synapse:4020/internal/pr/register",
		},
		want: map[string]string{
			"EriURL":        "http://env.eri:8088",
			"EriLLMURL":     "http://env.eri:8088/internal/llm",
			"PRRegisterURL": "http://env.synapse:4020/internal/pr/register",
		},
	},
	{
		name: "synapse pr register url env fallback",
		yaml: covenDarkBaseConfig,
		env:  map[string]string{"SYNAPSE_PR_REGISTER_URL": "http://fallback.synapse:4020/internal/pr/register"},
		want: map[string]string{
			"EriURL":        "http://eri.aerial",
			"EriLLMURL":     "http://eri.aerial/internal/llm",
			"PRRegisterURL": "http://fallback.synapse:4020/internal/pr/register",
		},
	},
	{
		name: "decomposed POSTGRES_PASSWORD builds database url",
		yaml: covenConfigSwap(covenDarkBaseConfig, "database_url: 'postgres://localhost/coven_test'", "database_url: ''"),
		env:  map[string]string{"POSTGRES_PASSWORD": "secretpass"},
		want: map[string]string{
			"Port":        "4011",
			"DatabaseURL": "postgres://aerial:secretpass@postgres:5432/aerial?sslmode=disable",
		},
	},
	{
		name:    "missing database url returns error",
		yaml:    covenConfigSwap(covenDarkBaseConfig, "database_url: 'postgres://localhost/coven_test'", "database_url: ''"),
		wantErr: "config: database connection string is required",
	},
	{
		name: "secrets and repo fields load from yaml",
		yaml: covenDarkBaseConfig + "github_pat: 'pat-from-yaml'\nsystem_channel_id: '1562837549255495693'\n",
		want: map[string]string{
			"CustomSkillsDir": "/custom/skills",
			"GitHubPAT":       "pat-from-yaml",
			"ConfigRepoOwner": "example-owner",
			"ConfigRepoName":  "example-config",
			"PRRegisterURL":   "http://synapse.aerial/internal/pr/register",
			"SystemChannelID": "1562837549255495693",
		},
	},
	{
		name: "secret env overrides yaml and non-secret env is ignored",
		yaml: covenDarkBaseConfig + "github_pat: 'pat-from-yaml'\nsystem_channel_id: '1562837549255495693'\n",
		env: map[string]string{
			"CUSTOM_SKILLS_DIR":     "/env/custom-skills",
			"GITHUB_PAT":            "pat-from-env",
			"CONFIG_REPO_OWNER":     "env-owner",
			"CONFIG_REPO_NAME":      "env-repo",
			"BRAIN_PR_REGISTER_URL": "http://env-brain/pr/reg",
			"SYSTEM_CHANNEL_ID":     "1562837549255495694",
		},
		want: map[string]string{
			"CustomSkillsDir": "/custom/skills",
			"GitHubPAT":       "pat-from-env",
			"ConfigRepoOwner": "example-owner",
			"ConfigRepoName":  "example-config",
			"PRRegisterURL":   "http://synapse.aerial/internal/pr/register",
			"SystemChannelID": "1562837549255495694",
		},
	},
	{
		name: "non-numeric system channel id is cleared",
		yaml: covenDarkBaseConfig + "github_pat: 'pat-from-yaml'\nsystem_channel_id: '1562837549255495693'\n",
		env:  map[string]string{"SYSTEM_CHANNEL_ID": "general-channel-name"},
		want: map[string]string{"GitHubPAT": "pat-from-yaml", "SystemChannelID": ""},
	},
	{name: "empty path returns error", wantErr: "config: path cannot be empty"},
	{name: "nonexistent path returns error", path: "nonexistent.yaml", wantErr: "config: failed to read "},
	{
		name:    "missing eri_url returns error",
		yaml:    covenConfigSwap(covenDarkBaseConfig, "eri_url: 'http://eri.aerial'", "eri_url: ''"),
		wantErr: "config: eri_url is required in ",
	},
	{
		name:    "missing eri_llm_url returns error",
		yaml:    covenConfigSwap(covenDarkBaseConfig, "eri_llm_url: 'http://eri.aerial/internal/llm'", "eri_llm_url: ''"),
		wantErr: "config: eri_llm_url is required in ",
	},
	{
		name:    "missing pr_register_url returns error",
		yaml:    covenConfigSwap(covenDarkBaseConfig, "pr_register_url: 'http://synapse.aerial/internal/pr/register'", "pr_register_url: ''"),
		wantErr: "config: pr_register_url is required in ",
	},
	{
		name:    "missing custom_skills_dir returns error",
		yaml:    covenConfigSwap(covenDarkBaseConfig, "custom_skills_dir: '/custom/skills'", "custom_skills_dir: ''"),
		wantErr: "config: custom_skills_dir is required in ",
	},
	{
		name:    "missing config_repo_owner returns error",
		yaml:    covenConfigSwap(covenDarkBaseConfig, "config_repo_owner: 'example-owner'", "config_repo_owner: ''"),
		wantErr: "config: config_repo_owner is required in ",
	},
	{
		name:    "missing config_repo_name returns error",
		yaml:    covenConfigSwap(covenDarkBaseConfig, "config_repo_name: 'example-config'", "config_repo_name: ''"),
		wantErr: "config: config_repo_name is required in ",
	},
	{
		name:    "missing ollama base_url returns error",
		yaml:    covenConfigSwap(covenDarkBaseConfig, "base_url: 'http://senses.aerial:8080'", "base_url: ''"),
		wantErr: "config: ollama.base_url is required in ",
	},
	{
		name:    "missing ollama model returns error",
		yaml:    covenConfigSwap(covenDarkBaseConfig, "model: 'bge-small-en-v1.5'", "model: ''"),
		wantErr: "config: ollama.model is required in ",
	},
	{
		name:    "zero ollama num_ctx returns error",
		yaml:    covenConfigSwap(covenDarkBaseConfig, "num_ctx: 512", "num_ctx: 0"),
		wantErr: "config: ollama.num_ctx must be positive in ",
	},
}

func TestLoadConfigFile(t *testing.T) {
	t.Parallel()
	for _, tc := range covenLoadConfigCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := covenLoadConfig(t, tc.yaml, tc.path, tc.dbArg, tc.env)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error = %v", err)
			}
			covenAssertFields(t, cfg, tc.want)
		})
	}
}

func TestNilConfigDarkDefaults(t *testing.T) {
	var cfg *Config
	if got := cfg.EffectiveStack(); got != DefaultStack {
		t.Errorf("nil EffectiveStack() = %q, want %q", got, DefaultStack)
	}
	if got := cfg.ExtractTimeout(); got != DefaultExtractTimeout {
		t.Errorf("nil ExtractTimeout() = %v, want %v", got, DefaultExtractTimeout)
	}
	if got := cfg.ExtractBatchLimit(); got != DefaultExtractMaxSteps {
		t.Errorf("nil ExtractBatchLimit() = %d, want %d", got, DefaultExtractMaxSteps)
	}
}

func TestNormalizeStack(t *testing.T) {
	tests := []struct {
		name  string
		stack string
		want  string
	}{
		{name: "empty defaults to aerial", stack: "", want: "aerial"},
		{name: "whitespace defaults to aerial", stack: "   ", want: "aerial"},
		{name: "keeps explicit stack", stack: "canary", want: "canary"},
		{name: "trims padding", stack: "  aerial  ", want: "aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizeStack(tc.stack); got != tc.want {
				t.Errorf("NormalizeStack(%q) = %q, want %q", tc.stack, got, tc.want)
			}
		})
	}
}

// mapEnv returns a getenv backed by m so loaders never read the process env.
func mapEnv(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestResolveConfigPath(t *testing.T) {
	oldLocal := localConfigPath
	defer func() {
		localConfigPath = oldLocal
	}()
	tmpDir := t.TempDir()
	existingLocal := filepath.Join(tmpDir, "local.yaml")
	if err := os.WriteFile(existingLocal, []byte("port: '4015'\n"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	missingLocal := filepath.Join(tmpDir, "nonexistent.yaml")

	tests := []struct {
		name      string
		localPath string
		envPath   string
		fallback  string
		want      string
	}{
		{
			name:      "EnvConfigPathSet",
			localPath: missingLocal,
			envPath:   "/custom/path.yaml",
			fallback:  "/default/path.yaml",
			want:      "/custom/path.yaml",
		},
		{
			name:      "LocalConfigFileExists",
			localPath: existingLocal,
			fallback:  "/default/path.yaml",
			want:      existingLocal,
		},
		{
			name:      "LocalConfigPrecedesDefaultConfigPath",
			localPath: existingLocal,
			fallback:  DefaultConfigPath,
			want:      existingLocal,
		},
		{
			name:      "FallbackUsed",
			localPath: missingLocal,
			fallback:  "/default/path.yaml",
			want:      "/default/path.yaml",
		},
		{
			name:      "EmptyFallbackUsesDefaultConfigPath",
			localPath: missingLocal,
			fallback:  "",
			want:      DefaultConfigPath,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			localConfigPath = tc.localPath
			t.Setenv("CONFIG_PATH", tc.envPath)
			got := ResolveConfigPath(tc.fallback)
			if got != tc.want {
				t.Errorf("ResolveConfigPath() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestScopedThreadKey(t *testing.T) {
	tests := []struct {
		name     string
		stack    string
		threadID string
		want     string
	}{
		{name: "explicit stack scopes key", stack: "canary", threadID: "th-1", want: "canary/th-1"},
		{name: "empty stack defaults to aerial", stack: "", threadID: "th-1", want: "aerial/th-1"},
		{name: "whitespace stack defaults to aerial", stack: "  ", threadID: "th-1", want: "aerial/th-1"},
		{name: "trims padding", stack: "  aerial  ", threadID: "  th-1 ", want: "aerial/th-1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ScopedThreadKey(tc.stack, tc.threadID); got != tc.want {
				t.Errorf("ScopedThreadKey(%q, %q) = %q, want %q", tc.stack, tc.threadID, got, tc.want)
			}
		})
	}
}

func TestParseScopedThreadKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		key        string
		wantAgent  string
		wantThread string
	}{
		{name: "standard scoped key", key: "agent/th-123", wantAgent: "agent", wantThread: "th-123"},
		{name: "unscoped thread key", key: "th-456", wantAgent: "aerial", wantThread: "th-456"},
		{name: "empty key", key: "", wantAgent: "aerial", wantThread: ""},
		{name: "multi slash key", key: "aerial/dev/th-789", wantAgent: "aerial", wantThread: "dev/th-789"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotAgent, gotThread := ParseScopedThreadKey(tc.key)
			if gotAgent != tc.wantAgent || gotThread != tc.wantThread {
				t.Fatalf("ParseScopedThreadKey(%q) = (%q, %q), want (%q, %q)", tc.key, gotAgent, gotThread, tc.wantAgent, tc.wantThread)
			}
		})
	}
}
