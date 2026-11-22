package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/transcripts/internal/config"
)

func TestMainFunc(t *testing.T) {
	origConfigPath := configPath
	origLoadConfigFn := loadConfigFn
	origRunAppFn := runAppFn
	origExitFn := exitFn
	t.Cleanup(func() {
		configPath = origConfigPath
		loadConfigFn = origLoadConfigFn
		runAppFn = origRunAppFn
		exitFn = origExitFn
	})

	tests := []struct {
		name       string
		configErr  bool
		runAppErr  error
		wantExit   bool
		wantSubstr string
	}{
		{
			name:      "successful run completes without exit",
			runAppErr: nil,
			wantExit:  false,
		},
		{
			name:      "server closed error completes without exit",
			runAppErr: http.ErrServerClosed,
			wantExit:  false,
		},
		{
			name:       "config error triggers fatal exit",
			configErr:  true,
			wantExit:   true,
			wantSubstr: "Fatal configuration error",
		},
		{
			name:       "run app error triggers fatal exit",
			runAppErr:  errors.New("listen error"),
			wantExit:   true,
			wantSubstr: "Server failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var exitCalled bool
			var exitMsg string

			exitFn = func(format string, v ...any) {
				exitCalled = true
				exitMsg = fmt.Sprintf(format, v...)
			}

			loadConfigFn = func(string) (*config.Config, error) {
				if tc.configErr {
					return nil, errors.New("cannot load configuration")
				}
				return &config.Config{Port: "8080"}, nil
			}

			runAppFn = func(context.Context, *config.Config) error {
				return tc.runAppErr
			}

			main()

			if exitCalled != tc.wantExit {
				t.Errorf("exitCalled = %v, want %v", exitCalled, tc.wantExit)
			}
			if tc.wantSubstr != "" && !strings.Contains(exitMsg, tc.wantSubstr) {
				t.Errorf("exitMsg = %q, want substring %q", exitMsg, tc.wantSubstr)
			}
		})
	}
}
