package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/coven/internal/config"
)

func mapEnv(m map[string]string) func(string) string {
	return func(k string) string {
		return m[k]
	}
}

// TestRunMain covers coven-specific wiring (getenv bound into LoadConfig);
// the generic bootstrap branches are covered by httpkit.TestRunService.
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
  base_url: "http://senses.aerial:8080"
  model: "bge-small-en-v1.5"
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

// TestMainFunc checks main binds os.Getenv into runMain.
func TestMainFunc(t *testing.T) {
	oldRun, oldExit, oldPath := runAppFn, exitFn, configPath
	t.Cleanup(func() { runAppFn, exitFn, configPath = oldRun, oldExit, oldPath })

	testCfg := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(testCfg, []byte("port: \"4008\"\neri_url: \"http://eri.aerial\"\neri_llm_url: \"http://eri.aerial/internal/llm\"\npr_register_url: \"http://synapse.aerial/internal/pr/register\"\ncustom_skills_dir: \"/config/custom-skills\"\nrules_dir: \"/config/rules\"\nconfig_repo_owner: \"example\"\nconfig_repo_name: \"example-config\"\nollama:\n  base_url: \"http://senses.aerial:8080\"\n  model: \"bge-small-en-v1.5\"\n  num_ctx: 512\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	configPath = testCfg
	t.Setenv("CONFIG_PATH", testCfg)
	t.Setenv("DATABASE_URL", "postgres://env-user:pass@localhost:5432/db")

	gotDB := ""
	exitCalled := false
	exitFn = func(string, ...any) { exitCalled = true }
	runAppFn = func(_ context.Context, cfg *config.Config) error {
		gotDB = cfg.DatabaseURL
		return nil
	}
	main()
	if !strings.Contains(gotDB, "env-user") {
		t.Errorf("runAppFn cfg.DatabaseURL = %q, want env value", gotDB)
	}
	if exitCalled {
		t.Error("exitFn called on clean run")
	}
}
