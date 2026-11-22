package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/azylman/aerial/mcp/docket-mcp/internal/config"
)

func TestMain_Execution_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()
	badConfigPath := filepath.Join(tmpDir, "bad_config.yaml")
	_ = os.WriteFile(badConfigPath, []byte("port: [unclosed"), 0o600)

	tests := []struct {
		name             string
		configPath       string
		runErr           error
		useRealRunServer bool
		wantExit         bool
	}{
		{name: "success exit", configPath: "", runErr: nil, wantExit: false},
		{name: "server closed error", configPath: "", runErr: http.ErrServerClosed, wantExit: false},
		{name: "fatal error calls exitFn", configPath: "", runErr: errors.New("fatal server error"), wantExit: true},
		{name: "config load error calls exitFn", configPath: badConfigPath, runErr: nil, wantExit: true},
		{name: "real runServer failure calls exitFn", configPath: "", useRealRunServer: true, wantExit: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldRun := runServerFn
			oldExit := exitFn
			exitCalled := false

			t.Setenv("CONFIG_PATH", tc.configPath)

			if !tc.useRealRunServer {
				runServerFn = func(ctx context.Context, cfg *config.Config) error {
					return tc.runErr
				}
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
