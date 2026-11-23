package main

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"testing"

	"go.uber.org/goleak"

	"github.com/azylman/aerial/services/eri/internal/server"
	"github.com/azylman/aerial/services/eri/pkg/config"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleak.Cleanup(func(exitCode int) {
		http.DefaultClient.CloseIdleConnections()
		if cl, ok := http.DefaultTransport.(interface{ CloseIdleConnections() }); ok {
			cl.CloseIdleConnections()
		}
	}))
}

func TestMainEntrypoint(t *testing.T) {
	origLoad, origRun, origFatal := loadConfigFn, runAppFn, fatalfFn
	t.Cleanup(func() {
		loadConfigFn, runAppFn, fatalfFn = origLoad, origRun, origFatal
	})

	loadConfigFn = func() (*config.Config, error) {
		return config.NewFromData(config.DefaultConfigData()), nil
	}
	runAppFn = func(context.Context, *config.Config, ...server.BrainAppOption) error {
		return nil
	}
	var fatalMsg string
	fatalfFn = func(format string, v ...any) {
		fatalMsg = format
	}

	execMain := main
	execMain()
	if fatalMsg != "" {
		t.Errorf("unexpected fatal message: %s", fatalMsg)
	}

	runAppFn = func(context.Context, *config.Config, ...server.BrainAppOption) error {
		return errors.New("startup failure")
	}
	execMain()
	if fatalMsg != "Server failed: %v" {
		t.Errorf("fatalf format = %q, want Server failed: %%v", fatalMsg)
	}
}

type runMainTestCase struct {
	name         string
	cfgLoadErr   bool
	emptyCfg     bool
	runAppErr    error
	cancelCtx    bool
	wantErr      bool
	wantErrIs    error
	wantHydrated bool
}

func verifyHydratedConfig(t *testing.T, cfg *config.Config) {
	t.Helper()
	cur := cfg.Current()
	if cur.GeminiHomeDir == "" {
		t.Errorf("expected GeminiHomeDir to be hydrated")
	}
	if cur.DataDir == "" {
		t.Errorf("expected DataDir to be hydrated")
	}
	if cur.Port != "8080" {
		t.Errorf("expected Port 8080, got %q", cur.Port)
	}
}

func assertRunMainCase(t *testing.T, tc runMainTestCase, tracingPath string) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if tc.cancelCtx {
		cancel()
	}

	loadCfg := func() (*config.Config, error) {
		if tc.cfgLoadErr {
			return nil, tc.wantErrIs
		}
		d := config.DefaultConfigData()
		if tc.emptyCfg {
			d.GeminiHomeDir = ""
			d.DataDir = ""
			d.Port = ""
		}
		return config.NewFromData(d), nil
	}

	var capturedCfg *config.Config
	runApp := func(c context.Context, cfg *config.Config, opts ...server.BrainAppOption) error {
		capturedCfg = cfg
		if tc.cancelCtx && c.Err() == nil {
			t.Errorf("expected canceled context inside runApp")
		}
		return tc.runAppErr
	}

	err := runMain(ctx, loadCfg, runApp, tracingPath)
	if tc.wantErr {
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
			t.Errorf("got error %v, want %v", err, tc.wantErrIs)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.wantHydrated && capturedCfg != nil {
		verifyHydratedConfig(t, capturedCfg)
	}
}

func TestRunMain(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tracingPath := filepath.Join(tmpDir, "tracing.yaml")

	errExpected := errors.New("boom")

	tests := []runMainTestCase{
		{
			name:       "config load failure",
			cfgLoadErr: true,
			wantErr:    true,
			wantErrIs:  errExpected,
		},
		{
			name:         "default config directory and port hydration",
			emptyCfg:     true,
			wantErr:      false,
			wantHydrated: true,
		},
		{
			name:      "runApp returns error",
			runAppErr: errExpected,
			wantErr:   true,
			wantErrIs: errExpected,
		},
		{
			name:      "http.ErrServerClosed clean exit",
			runAppErr: http.ErrServerClosed,
			wantErr:   false,
		},
		{
			name:      "context cancellation cleanly runs app",
			cancelCtx: true,
			wantErr:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertRunMainCase(t, tc, tracingPath)
		})
	}
}
