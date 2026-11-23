package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/azylman/aerial/mcp/kernel-mcp/internal/config"
)

func TestMain_Execution_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()
	absentConfigPath := filepath.Join(tmpDir, "absent.yaml")
	badConfigPath := filepath.Join(tmpDir, "bad_config.yaml")
	if err := os.WriteFile(badConfigPath, []byte("port: [unclosed"), 0o600); err != nil {
		t.Fatalf("write bad config: %v", err)
	}

	tests := []struct {
		name       string
		configPath string
		runErr     error
		wantExit   bool
	}{
		{"success exit", absentConfigPath, nil, false},
		{"server closed error", absentConfigPath, http.ErrServerClosed, false},
		{"fatal error calls exitFn", absentConfigPath, errors.New("fatal server error"), true},
		{"config load error calls exitFn", badConfigPath, nil, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldRun := runServerFn
			oldExit := exitFn
			exitCalled := false
			t.Setenv("CONFIG_PATH", tc.configPath)
			runServerFn = func(ctx context.Context, cfg *config.Config) error {
				return tc.runErr
			}
			exitFn = func(format string, v ...any) {
				exitCalled = true
			}
			t.Cleanup(func() {
				runServerFn = oldRun
				exitFn = oldExit
			})

			main()

			if exitCalled != tc.wantExit {
				t.Fatalf("exitCalled = %v, want %v", exitCalled, tc.wantExit)
			}
		})
	}
}
