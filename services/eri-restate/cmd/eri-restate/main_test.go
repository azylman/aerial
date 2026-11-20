package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri-restate/internal/app"
)

func TestMain_Execution(t *testing.T) {
	origLoad, origRun, origExit, origTracing := loadConfigFn, runAppFn, exitFn, initTracingFn
	t.Cleanup(func() {
		loadConfigFn, runAppFn, exitFn, initTracingFn = origLoad, origRun, origExit, origTracing
	})

	tests := []struct {
		name       string
		runErr     error
		wantExited bool
	}{
		{
			name:       "success case does not exit",
			runErr:     nil,
			wantExited: false,
		},
		{
			name:       "error case calls exit",
			runErr:     errors.New("startup failed"),
			wantExited: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var tracedService string
			flushes := 0
			initTracingFn = func(_ context.Context, service, _ string) (tracing.Config, func()) {
				tracedService = service
				return tracing.Config{}, func() { flushes++ }
			}
			loadConfigFn = func() app.Config {
				return app.Config{HealthPort: "4028", RestatePort: "9080", AgentName: "aerial"}
			}
			runAppFn = func(ctx context.Context, cfg app.Config, opts ...app.AppOption) error {
				return tc.runErr
			}
			exited := false
			exitFn = func(format string, v ...any) {
				exited = true
			}

			main()

			assert.Equal(t, tc.wantExited, exited)
			assert.Equal(t, "eri-restate", tracedService)
			assert.Equal(t, 1, flushes)
		})
	}
}
