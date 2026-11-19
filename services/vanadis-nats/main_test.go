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
	defer func() {
		loadConfigFn, runAppFn, exitFn, initTracingFn = origLoad, origRun, origExit, origTracing
	}()

	loadConfigFn = func() config.Config {
		return config.Config{Port: "4027", NATSURL: "nats://localhost:4222"}
	}
	initTracingFn = func(context.Context, string, string) (tracing.Config, func()) {
		return tracing.Config{}, func() {}
	}

	tests := []struct {
		name       string
		runAppErr  error
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:      "success case",
			runAppErr: nil,
			wantErr:   false,
		},
		{
			name:       "failure case returns error",
			runAppErr:  errors.New("fatal run app error"),
			wantErr:    true,
			wantErrMsg: "fatal run app error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runAppFn = func(context.Context, config.Config) error {
				return tc.runAppErr
			}

			err := runMain()
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestMain_Execution(t *testing.T) {
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

	runAppFn = func(context.Context, config.Config) error {
		return nil
	}
	var exitCalled bool
	exitFn = func(string, ...any) {
		exitCalled = true
	}
	main()
	assert.False(t, exitCalled)

	runAppFn = func(context.Context, config.Config) error {
		return errors.New("startup error")
	}
	main()
	assert.True(t, exitCalled)
}
