package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/config"
)

// TestMain_ExitPaths drives main() through every RunApp outcome. It swaps
// package-level seams, so it cannot run in parallel.
func TestMain_ExitPaths(t *testing.T) {
	origLoad, origStrict, origTracing, origRun, origFatal := loadConfigFn, loadConfigStrictFn, initTracingFn, runAppFn, fatalfFn
	t.Cleanup(func() {
		loadConfigFn, loadConfigStrictFn, initTracingFn, runAppFn, fatalfFn = origLoad, origStrict, origTracing, origRun, origFatal
	})
	loadConfigFn = func() config.Config { return config.Config{Port: "4020"} }
	loadConfigStrictFn = func() (*config.Config, error) { return &config.Config{Port: "4020"}, nil }

	tests := []struct {
		name      string
		runAppErr error
		wantFatal string
		wantFlush bool
	}{
		{name: "clean exit", runAppErr: nil, wantFlush: true},
		{name: "server closed is a clean exit", runAppErr: http.ErrServerClosed, wantFlush: true},
		{name: "context canceled is a graceful shutdown", runAppErr: context.Canceled, wantFlush: true},
		{name: "startup error is fatal", runAppErr: errors.New("startup failed"), wantFatal: "[synapse] Error: startup failed", wantFlush: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var flushed bool
			var fatal string
			initTracingFn = func(context.Context, string, string) (tracing.Config, func()) {
				return tracing.Config{}, func() { flushed = true }
			}
			runAppFn = func(context.Context, *httpkit.Reloadable[config.Config]) error { return tc.runAppErr }
			fatalfFn = func(format string, v ...any) { fatal = fmt.Sprintf(format, v...) }

			main()
			if fatal != tc.wantFatal {
				t.Errorf("fatalf = %q, want %q", fatal, tc.wantFatal)
			}
			if flushed != tc.wantFlush {
				t.Errorf("tracing flushed = %v, want %v", flushed, tc.wantFlush)
			}
		})
	}
}
