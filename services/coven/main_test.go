package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/azylman/aerial/services/coven/internal/config"
)

func TestMain_Execution(t *testing.T) {
	oldRun, oldExit, oldPath := runAppFn, exitFn, configPath
	defer func() {
		runAppFn, exitFn, configPath = oldRun, oldExit, oldPath
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
config_repo_owner: "example"
config_repo_name: "example-config"
ollama:
  base_url: "http://ollama:11434"
  model: "all-minilm"
  num_ctx: 512
`
	_ = os.WriteFile(testCfg, []byte(testCfgContent), 0o644)
	configPath = testCfg
	t.Setenv("CONFIG_PATH", testCfg)
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")

	runCalled := false
	runAppFn = func(ctx context.Context, cfg *config.Config) error {
		runCalled = true
		return nil
	}
	main()
	if !runCalled {
		t.Error("expected runAppFn called")
	}

	exitCalled := false
	exitFn = func(string, ...any) { exitCalled = true }
	runAppFn = func(ctx context.Context, cfg *config.Config) error { return http.ErrServerClosed }
	main()
	if exitCalled {
		t.Error("did not expect exitFn on http.ErrServerClosed")
	}

	runAppFn = func(ctx context.Context, cfg *config.Config) error { return context.DeadlineExceeded }
	main()
	if !exitCalled {
		t.Error("expected exitFn on server error")
	}

	exitCalled = false
	configPath = filepath.Join(tmpDir, "nonexistent.yaml")
	t.Setenv("CONFIG_PATH", configPath)
	main()
	if !exitCalled {
		t.Error("expected exitFn on config load error")
	}
}

func mapEnv(m map[string]string) func(string) string {
	return func(k string) string {
		return m[k]
	}
}

func TestRunMain(t *testing.T) {
	oldRun, oldExit, oldPath := runAppFn, exitFn, configPath
	defer func() {
		runAppFn, exitFn, configPath = oldRun, oldExit, oldPath
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
config_repo_owner: "example"
config_repo_name: "example-config"
ollama:
  base_url: "http://ollama:11434"
  model: "all-minilm"
  num_ctx: 512
`
	if err := os.WriteFile(testCfg, []byte(testCfgContent), 0o644); err != nil {
		t.Fatal(err)
	}
	missingTracing := filepath.Join(tmpDir, "missing-tracing.yaml")
	dbURL := "postgres://user:pass@localhost:5432/db"

	tests := []struct {
		name         string
		cfgPath      string
		env          map[string]string
		runErr       error
		wantRunCall  bool
		wantExitCall bool
	}{
		{name: "CleanExit", cfgPath: testCfg, env: map[string]string{"DATABASE_URL": dbURL}, wantRunCall: true},
		{name: "ErrServerClosedIgnored", cfgPath: testCfg, env: map[string]string{"DATABASE_URL": dbURL}, runErr: http.ErrServerClosed, wantRunCall: true},
		{name: "RuntimeFailureCallsExit", cfgPath: testCfg, env: map[string]string{"DATABASE_URL": dbURL}, runErr: context.DeadlineExceeded, wantRunCall: true, wantExitCall: true},
		{name: "ConfigLoadFailureCallsExit", cfgPath: filepath.Join(tmpDir, "nonexistent.yaml"), wantExitCall: true},
		{name: "InvalidEnvCallsExit", cfgPath: testCfg, env: map[string]string{"DATABASE_URL": dbURL, "K8S_AUTH_ENABLED": "maybe"}, wantExitCall: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configPath = tc.cfgPath
			t.Setenv("CONFIG_PATH", tc.cfgPath)

			runCalled := false
			exitCalled := false
			runAppFn = func(ctx context.Context, cfg *config.Config) error {
				runCalled = true
				return tc.runErr
			}
			exitFn = func(string, ...any) {
				exitCalled = true
			}

			runMain(mapEnv(tc.env), missingTracing)
			if runCalled != tc.wantRunCall {
				t.Errorf("runCalled = %v, want %v", runCalled, tc.wantRunCall)
			}
			if exitCalled != tc.wantExitCall {
				t.Errorf("exitCalled = %v, want %v", exitCalled, tc.wantExitCall)
			}
		})
	}
}
