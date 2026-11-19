package main

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/config"
)

func TestRunMain(t *testing.T) {
	origLoad, origStrict, origTracing, origRun := loadConfigFn, loadConfigStrictFn, initTracingFn, runAppFn
	defer func() {
		loadConfigFn, loadConfigStrictFn, initTracingFn, runAppFn = origLoad, origStrict, origTracing, origRun
	}()

	loadConfigFn = func() config.Config {
		return config.Config{Port: "4020"}
	}
	loadConfigStrictFn = func() (*config.Config, error) {
		return &config.Config{Port: "4020"}, nil
	}
	var flushCalled bool
	initTracingFn = func(ctx context.Context, service, path string) (tracing.Config, func()) {
		return tracing.Config{}, func() { flushCalled = true }
	}

	tests := []struct {
		name      string
		runAppErr error
		wantErr   bool
	}{
		{
			name:      "success clean exit",
			runAppErr: nil,
			wantErr:   false,
		},
		{
			name:      "server closed treated as clean exit",
			runAppErr: http.ErrServerClosed,
			wantErr:   false,
		},
		{
			name:      "fatal error propagated",
			runAppErr: errors.New("startup failed"),
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			flushCalled = false
			runAppFn = func(ctx context.Context, cfgSrc *httpkit.Reloadable[config.Config]) error {
				return tc.runAppErr
			}

			err := runMain()
			if (err != nil) != tc.wantErr {
				t.Fatalf("runMain() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !flushCalled {
				t.Errorf("expected flushTracing to be called")
			}
		})
	}
}

func TestMainExecution(t *testing.T) {
	origLoad, origStrict, origTracing, origRun, origFatal := loadConfigFn, loadConfigStrictFn, initTracingFn, runAppFn, fatalfFn
	defer func() {
		loadConfigFn, loadConfigStrictFn, initTracingFn, runAppFn, fatalfFn = origLoad, origStrict, origTracing, origRun, origFatal
	}()

	loadConfigFn = func() config.Config { return config.Config{Port: "4020"} }
	loadConfigStrictFn = func() (*config.Config, error) { return &config.Config{Port: "4020"}, nil }
	initTracingFn = func(ctx context.Context, service, path string) (tracing.Config, func()) {
		return tracing.Config{}, func() {}
	}

	var fatalCalled bool
	var fatalErr error
	fatalfFn = func(format string, v ...any) {
		fatalCalled = true
		if len(v) > 0 {
			if err, ok := v[0].(error); ok {
				fatalErr = err
			}
		}
	}

	// 1. Success case
	runAppFn = func(ctx context.Context, cfgSrc *httpkit.Reloadable[config.Config]) error {
		return nil
	}
	fatalCalled = false
	main()
	if fatalCalled {
		t.Errorf("unexpected fatalf call on success")
	}

	// 2. Canceled context (graceful shutdown)
	runAppFn = func(ctx context.Context, cfgSrc *httpkit.Reloadable[config.Config]) error {
		return context.Canceled
	}
	fatalCalled = false
	main()
	if fatalCalled {
		t.Errorf("unexpected fatalf call on context.Canceled")
	}

	// 3. Error case triggers fatalf
	testErr := errors.New("boom")
	runAppFn = func(ctx context.Context, cfgSrc *httpkit.Reloadable[config.Config]) error {
		return testErr
	}
	fatalCalled = false
	fatalErr = nil
	main()
	if !fatalCalled {
		t.Errorf("expected fatalf call on error")
	}
	if !errors.Is(fatalErr, testErr) {
		t.Errorf("fatalErr = %v, want %v", fatalErr, testErr)
	}
}
