package main

import (
	"context"
	"errors"
	"testing"

	"github.com/azylman/aerial/services/console/internal/config"
)

func TestMain_Execution(t *testing.T) {
	origLoad := loadConfigFn
	origRun := runConsoleFn
	origExit := exitFn
	origPath := defaultConfigPath
	t.Cleanup(func() {
		loadConfigFn = origLoad
		runConsoleFn = origRun
		exitFn = origExit
		defaultConfigPath = origPath
	})

	errConfig := errors.New("load config boom")
	errRun := errors.New("run console boom")

	tests := []struct {
		name       string
		cfgErr     error
		runErr     error
		wantExit   bool
		wantErrSub string
	}{
		{
			name:       "load config failure exits",
			cfgErr:     errConfig,
			wantExit:   true,
			wantErrSub: "failed to load config",
		},
		{
			name:       "run console failure exits",
			runErr:     errRun,
			wantExit:   true,
			wantErrSub: "console failed",
		},
		{
			name:     "successful startup and run",
			wantExit: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var exitCalled bool
			var exitMsg string

			exitFn = func(format string, args ...any) {
				exitCalled = true
				if len(args) > 0 {
					exitMsg = format
				}
			}

			loadConfigFn = func(path string) (config.ConsoleConfig, error) {
				if tc.cfgErr != nil {
					return config.ConsoleConfig{}, tc.cfgErr
				}
				return config.ConsoleConfig{Port: "8080"}, nil
			}

			runConsoleFn = func(ctx context.Context, cfg config.ConsoleConfig) error {
				return tc.runErr
			}

			main()

			if exitCalled != tc.wantExit {
				t.Fatalf("exitCalled = %v, want %v (msg: %s)", exitCalled, tc.wantExit, exitMsg)
			}
			if tc.wantErrSub != "" && exitMsg != tc.wantErrSub+": %v" {
				t.Errorf("exitMsg format = %q, want prefix %q", exitMsg, tc.wantErrSub)
			}
		})
	}
}
