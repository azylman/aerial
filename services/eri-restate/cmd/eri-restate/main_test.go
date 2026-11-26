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
		name        string
		tracingCfg  tracing.Config
		runErr      error
		wantExited  bool
		wantContent bool
	}{
		{
			name:        "success case does not exit without content capture",
			tracingCfg:  tracing.Config{Enabled: true, CaptureContent: false},
			runErr:      nil,
			wantExited:  false,
			wantContent: false,
		},
		{
			name:        "success case enables trace content when capture content configured",
			tracingCfg:  tracing.Config{Enabled: true, CaptureContent: true},
			runErr:      nil,
			wantExited:  false,
			wantContent: true,
		},
		{
			name:        "error case calls exit",
			tracingCfg:  tracing.Config{},
			runErr:      errors.New("startup failed"),
			wantExited:  true,
			wantContent: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var tracedService string
			var receivedCfg app.Config
			flushes := 0
			initTracingFn = func(_ context.Context, service, _ string) (tracing.Config, func()) {
				tracedService = service
				return tc.tracingCfg, func() { flushes++ }
			}
			loadConfigFn = func() app.Config {
				return app.Config{HealthPort: "4028", RestatePort: "9080", AgentName: "aerial"}
			}
			runAppFn = func(ctx context.Context, cfg app.Config, opts ...app.AppOption) error {
				receivedCfg = cfg
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
			assert.Equal(t, tc.wantContent, receivedCfg.TraceContent)
		})
	}
}
