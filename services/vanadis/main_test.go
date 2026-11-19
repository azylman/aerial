package main

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/vanadis/internal/config"
)

// TestMainEntrypoint pins the thin entrypoint contract: config loads once,
// the app runs with that config, graceful close exits 0, and real errors
// exit 1 without ever invoking the live server.
func TestMainEntrypoint(t *testing.T) {
	realLoad, realRun, realInit, realExit := loadConfigFn, runAppFn, initTracingFn, exitFn
	t.Cleanup(func() { loadConfigFn, runAppFn, initTracingFn, exitFn = realLoad, realRun, realInit, realExit })

	initTracingFn = func(context.Context, string, string) (tracing.Config, func()) {
		return tracing.Config{}, func() {}
	}
	loadConfigFn = func() config.Config { return config.Config{Port: "0"} }

	tests := []struct {
		name     string
		runErr   error
		wantCode int
	}{
		{name: "clean run exits zero", runErr: nil, wantCode: 0},
		{name: "graceful close exits zero", runErr: http.ErrServerClosed, wantCode: 0},
		{name: "wrapped close exits zero", runErr: errors.Join(http.ErrServerClosed), wantCode: 0},
		{name: "run error exits nonzero", runErr: errors.New("listen failed"), wantCode: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runAppFn = func(context.Context, config.Config) error { return tc.runErr }
			if got := run(); got != tc.wantCode {
				t.Fatalf("run() = %d, want %d (err %v)", got, tc.wantCode, tc.runErr)
			}
		})
	}
}

// TestMainForwardsRunExit pins the one behavior TestMainEntrypoint cannot:
// that a run error propagates through main to a nonzero process exit. The
// success-path mapping is owned by TestMainEntrypoint and not repeated here.
func TestMainForwardsRunExit(t *testing.T) {
	realLoad, realRun, realInit, realExit := loadConfigFn, runAppFn, initTracingFn, exitFn
	t.Cleanup(func() { loadConfigFn, runAppFn, initTracingFn, exitFn = realLoad, realRun, realInit, realExit })

	loadConfigFn = func() config.Config { return config.Config{} }
	initTracingFn = func(context.Context, string, string) (tracing.Config, func()) {
		return tracing.Config{}, func() {}
	}
	runAppFn = func(context.Context, config.Config) error { return errors.New("listen failed") }
	got := -1
	exitFn = func(code int) { got = code }
	main()
	if got != 1 {
		t.Fatalf("exit code = %d, want 1", got)
	}
}
