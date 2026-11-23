package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ClearConfigEnv pins every env var LoadConfig/LoadConfigFile or the NATS bus
// reads, so the pod or CI environment cannot leak into config tests.
func ClearConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"DATABASE_URL", "POSTGRES_URL", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB",
		"NATS_URL", "NATS_USER", "NATS_PASSWORD", "K8S_AUTH_ENABLED",
	} {
		t.Setenv(key, "")
	}
}

func TestResolveConfigPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	tests := []struct {
		name     string
		fallback string
		want     string
	}{
		{"explicit fallback returned when nothing exists", missing, missing},
		{"empty fallback uses default config path", "", "/config/transcripts.yaml"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CONFIG_PATH", "")
			oldLocal := localConfigPath
			localConfigPath = missing
			t.Cleanup(func() { localConfigPath = oldLocal })

			if got := ResolveConfigPath(tc.fallback); got != tc.want {
				t.Errorf("ResolveConfigPath(%q) = %q, want %q", tc.fallback, got, tc.want)
			}
		})
	}
}

// configView is the asserted subset of Config.
type configView struct {
	Port, Stack, DB, NATS          string
	Auth, Summarize                bool
	Batch, MaxSteps                int
	EriLLM, SensesURL, SensesModel string
}

func viewConfig(c *Config) configView {
	return configView{
		Port: c.Port, Stack: c.Stack, DB: c.DatabaseURL, NATS: c.NATSURL,
		Auth: c.Auth.Enabled, Summarize: c.Summarizer.Enabled,
		Batch: c.Summarizer.BatchLimit, MaxSteps: c.Summarizer.MaxSteps,
		EriLLM: c.Summarizer.EriLLMURL, SensesURL: c.Summarizer.SensesURL, SensesModel: c.Summarizer.SensesModel,
	}
}

func TestLoadConfigFile(t *testing.T) {
	const argDB = "postgres://arg:pass@host:5432/db"
	tests := []struct {
		name    string
		file    string // written under t.TempDir() when yaml is set; "" means an empty path
		yaml    string
		dbArg   string
		env     map[string]string
		want    configView
		wantErr string
	}{
		{
			name: "full config", file: "full.yaml", dbArg: argDB,
			yaml: "port: '4017'\nstack: 'aerial'\nnats_url: 'nats://nats:4222'\n",
			want: configView{Port: "4017", Stack: "aerial", DB: argDB, NATS: "nats://nats:4222", Batch: 10, MaxSteps: 50},
		},
		{
			name: "defaults fill port", file: "minimal.yaml", yaml: "stack: 'agent-alpha'\n", dbArg: argDB,
			want: configView{Port: "4017", Stack: "agent-alpha", DB: argDB, Batch: 10, MaxSteps: 50},
		},
		{
			name: "default stack when omitted", file: "nostack.yaml", yaml: "port: '4017'\ndatabase_url: 'postgres://x'\n",
			want: configView{Port: "4017", Stack: "aerial", DB: "postgres://x", Batch: 10, MaxSteps: 50},
		},
		{
			name: "deployed format with ollama", file: "deployed.yaml", dbArg: argDB,
			yaml: "port: '4030'\neri_llm_url: 'http://eri.example.com/llm'\nollama:\n  base_url: 'http://senses.example.com'\n  model: 'bge-small'\n",
			want: configView{
				Port: "4030", Stack: "aerial", DB: argDB, Batch: 10, MaxSteps: 50,
				EriLLM: "http://eri.example.com/llm", SensesURL: "http://senses.example.com", SensesModel: "bge-small",
			},
		},
		{
			name: "embedder format wins over ollama", file: "embedder.yaml", dbArg: argDB,
			yaml: "port: '4030'\nembedder:\n  base_url: 'http://embedder.example.com'\n  model: 'bge-custom'\nollama:\n  base_url: 'http://senses.example.com'\n",
			want: configView{
				Port: "4030", Stack: "aerial", DB: argDB, Batch: 10, MaxSteps: 50,
				SensesURL: "http://embedder.example.com", SensesModel: "bge-custom",
			},
		},
		{
			name: "yaml database url used when arg and env empty", file: "db.yaml", yaml: "database_url: 'postgres://yaml:pass@host:5432/db'\n",
			want: configView{Port: "4017", Stack: "aerial", DB: "postgres://yaml:pass@host:5432/db", Batch: 10, MaxSteps: 50},
		},
		{
			name: "env database url wins over yaml", file: "envdb.yaml", yaml: "database_url: 'postgres://yaml'\n",
			env:  map[string]string{"DATABASE_URL": "postgres://env:pass@host:5432/db"},
			want: configView{Port: "4017", Stack: "aerial", DB: "postgres://env:pass@host:5432/db", Batch: 10, MaxSteps: 50},
		},
		{
			name: "arg database url and env nats win", file: "env.yaml", yaml: "database_url: 'postgres://yaml'\nnats_url: 'nats://yaml:4222'\n",
			dbArg: argDB, env: map[string]string{"DATABASE_URL": "postgres://env", "NATS_URL": "nats://env:4222"},
			want: configView{Port: "4017", Stack: "aerial", DB: argDB, NATS: "nats://env:4222", Batch: 10, MaxSteps: 50},
		},
		{
			name: "summarizer keeps explicit bounds and urls", file: "sum.yaml", dbArg: argDB,
			yaml: "summarizer:\n  enabled: true\n  batch_limit: 3\n  max_steps: 7\n  eri_llm_url: 'http://eri:8080/llm'\n  senses_url: 'http://senses:8880'\n",
			want: configView{
				Port: "4017", Stack: "aerial", DB: argDB, Summarize: true, Batch: 3, MaxSteps: 7,
				EriLLM: "http://eri:8080/llm", SensesURL: "http://senses:8880",
			},
		},
		{
			name: "summarizer zero limits fill defaults", file: "sumzero.yaml", dbArg: argDB, yaml: "summarizer:\n  enabled: true\n",
			want: configView{Port: "4017", Stack: "aerial", DB: argDB, Summarize: true, Batch: 10, MaxSteps: 50},
		},
		{
			name: "auth enabled in yaml", file: "auth.yaml", dbArg: argDB, yaml: "auth:\n  enabled: true\n",
			want: configView{Port: "4017", Stack: "aerial", DB: argDB, Auth: true, Batch: 10, MaxSteps: 50},
		},
		{
			name: "auth enabled via env override", file: "authenv.yaml", dbArg: argDB, yaml: "stack: 'aerial'\n",
			env:  map[string]string{"K8S_AUTH_ENABLED": "true"},
			want: configView{Port: "4017", Stack: "aerial", DB: argDB, Auth: true, Batch: 10, MaxSteps: 50},
		},
		{
			name: "invalid auth env fails", file: "authbad.yaml", dbArg: argDB, yaml: "stack: 'aerial'\n",
			env: map[string]string{"K8S_AUTH_ENABLED": "maybe"}, wantErr: `invalid K8S_AUTH_ENABLED "maybe"`,
		},
		{name: "no database url anywhere fails", file: "nodb.yaml", yaml: "stack: 'aerial'\n", wantErr: "database connection string is required"},
		{name: "missing file", file: "missing.yaml", dbArg: argDB, wantErr: "failed to read"},
		{name: "malformed yaml", file: "malformed.yaml", yaml: "port: [unclosed", dbArg: argDB, wantErr: "failed to parse YAML"},
		{name: "empty path", dbArg: argDB, wantErr: "path cannot be empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ClearConfigEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			path := ""
			if tc.file != "" {
				path = filepath.Join(t.TempDir(), tc.file)
				if tc.yaml != "" {
					if err := os.WriteFile(path, []byte(tc.yaml), 0o644); err != nil {
						t.Fatalf("write fixture: %v", err)
					}
				}
			}

			cfg, err := LoadConfigFile(path, tc.dbArg)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("LoadConfigFile() err = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadConfigFile() err = %v", err)
			}
			if got := viewConfig(cfg); got != tc.want {
				t.Errorf("LoadConfigFile() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	ClearConfigEnv(t)
	t.Setenv("DATABASE_URL", "postgres://env:pass@host:5432/db")
	tmp := filepath.Join(t.TempDir(), "transcripts.yaml")
	if err := os.WriteFile(tmp, []byte("port: '4017'\n"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	cfg, err := LoadConfig(tmp)
	if err != nil {
		t.Fatalf("LoadConfig() err = %v", err)
	}
	if cfg.Port != "4017" || cfg.DatabaseURL != "postgres://env:pass@host:5432/db" {
		t.Fatalf("LoadConfig() unexpected cfg = %+v", cfg)
	}
}

func TestSummarizerConfigWithDefaults(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		input             SummarizerConfig
		wantBatchLimit    int
		wantMaxSteps      int
		wantIdleThreshold time.Duration
		wantMinStepDelta  int
	}{
		{
			name:              "zero values fill defaults",
			input:             SummarizerConfig{},
			wantBatchLimit:    DefaultSummarizerBatchLimit,
			wantMaxSteps:      DefaultSummarizerMaxSteps,
			wantIdleThreshold: DefaultSummarizerIdleThreshold,
			wantMinStepDelta:  DefaultSummarizerMinStepDelta,
		},
		{
			name: "custom values preserved",
			input: SummarizerConfig{
				BatchLimit:    5,
				MaxSteps:      100,
				IdleThreshold: 30 * time.Minute,
				MinStepDelta:  50,
			},
			wantBatchLimit:    5,
			wantMaxSteps:      100,
			wantIdleThreshold: 30 * time.Minute,
			wantMinStepDelta:  50,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.input.WithDefaults()
			if got.BatchLimit != tc.wantBatchLimit {
				t.Errorf("BatchLimit = %d, want %d", got.BatchLimit, tc.wantBatchLimit)
			}
			if got.MaxSteps != tc.wantMaxSteps {
				t.Errorf("MaxSteps = %d, want %d", got.MaxSteps, tc.wantMaxSteps)
			}
			if got.IdleThreshold != tc.wantIdleThreshold {
				t.Errorf("IdleThreshold = %v, want %v", got.IdleThreshold, tc.wantIdleThreshold)
			}
			if got.MinStepDelta != tc.wantMinStepDelta {
				t.Errorf("MinStepDelta = %d, want %d", got.MinStepDelta, tc.wantMinStepDelta)
			}
		})
	}
}
