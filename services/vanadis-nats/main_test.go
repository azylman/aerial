package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/vanadis-nats/internal/config"
)

func TestRunMain(t *testing.T) {
	origLoad, origRun, origExit, origTracing := loadConfigFn, runAppFn, exitFn, initTracingFn
	t.Cleanup(func() {
		loadConfigFn, runAppFn, exitFn, initTracingFn = origLoad, origRun, origExit, origTracing
	})

	loadConfigFn = func() config.Config {
		return config.Config{Port: "4027", NATSURL: "nats://localhost:4222"}
	}
	initTracingFn = func(context.Context, string, string) (tracing.Config, func()) {
		return tracing.Config{}, func() {}
	}

	tests := []struct {
		name      string
		runAppErr error
		wantErr   string
		wantExit  bool
	}{
		{name: "success returns nil and main does not exit", runAppErr: nil, wantErr: "", wantExit: false},
		{name: "run app error is returned and main exits", runAppErr: errors.New("fatal run app error"), wantErr: "fatal run app error", wantExit: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runAppFn = func(context.Context, config.Config) error { return tc.runAppErr }
			var exited bool
			exitFn = func(string, ...any) { exited = true }

			err := runMain()
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.wantErr)
			}
			main()
			assert.Equal(t, tc.wantExit, exited)
		})
	}
}
