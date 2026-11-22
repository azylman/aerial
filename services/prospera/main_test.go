package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/prospera/internal/config"
)

type runAppMainSignalTestCase struct {
	name         string
	runErr       error
	reloadCfg    *config.Config
	wantExit     bool
	wantStatus   httpkit.ReloadStatus
	wantCanceled bool
}

func TestRunApp_MainSignal(t *testing.T) {
	t.Cleanup(installTestTracing) // runMain runs tracing.Init, which replaces the test tracer
	origLoad := loadConfigFn
	origRun := runAppFn
	origExit := exitFn
	defer func() {
		loadConfigFn = origLoad
		runAppFn = origRun
		exitFn = origExit
	}()

	tests := []runAppMainSignalTestCase{
		{
			name:     "run app error triggers exit",
			runErr:   errors.New("mock run error"),
			wantExit: true,
		},
		{
			name:     "server closed error does not trigger exit",
			runErr:   http.ErrServerClosed,
			wantExit: false,
		},
		{
			name:     "clean execution",
			wantExit: false,
		},
		{
			name:         "port change requests graceful restart",
			reloadCfg:    &config.Config{Port: "9090", DatabaseURL: "sqlite://:memory:"},
			wantStatus:   "restarting",
			wantCanceled: true,
		},
		{
			name:       "live field change keeps running",
			reloadCfg:  &config.Config{Port: "4019", DatabaseURL: "sqlite://:memory:", DefaultDiscordChannel: "new-chan"},
			wantStatus: "reloaded",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runMainSignalCase(t, tc)
		})
	}
}

func runMainSignalCase(t *testing.T, tc runAppMainSignalTestCase) {
	t.Helper()
	exited := false
	exitFn = func(format string, v ...any) {
		exited = true
	}
	loads := 0
	loadConfigFn = func(_ string, getenv func(string) string) (*config.Config, error) {
		loads++
		if got := getenv("PROSPERA_STACK"); got != "alpha" {
			t.Errorf("loadConfigFn getenv(PROSPERA_STACK) = %q, want alpha", got)
		}
		if loads > 1 && tc.reloadCfg != nil {
			return tc.reloadCfg, nil
		}
		return &config.Config{Port: "4019", DatabaseURL: "sqlite://:memory:"}, nil
	}
	var gotStatus httpkit.ReloadStatus
	canceled := false
	runAppFn = func(ctx context.Context, cfgSrc *httpkit.Reloadable[config.Config]) error {
		if tc.reloadCfg == nil {
			return tc.runErr
		}
		gotStatus, _ = cfgSrc.Reload()
		if !tc.wantCanceled {
			canceled = ctx.Err() != nil
			return tc.runErr
		}
		select {
		case <-ctx.Done():
			canceled = true
		case <-time.After(5 * time.Second):
		}
		return tc.runErr
	}

	env := map[string]string{"PROSPERA_STACK": "alpha"}
	runMain(func(key string) string { return env[key] }, filepath.Join(t.TempDir(), "missing-tracing.yaml"))

	if exited != tc.wantExit {
		t.Errorf("runMain() exited = %v, want %v", exited, tc.wantExit)
	}
	if gotStatus != tc.wantStatus {
		t.Errorf("reload status = %q, want %q", gotStatus, tc.wantStatus)
	}
	if canceled != tc.wantCanceled {
		t.Errorf("run context canceled = %v, want %v", canceled, tc.wantCanceled)
	}
}

// TestMainEntry_ConfigErrorExits drives main() itself; the config load fails
// before tracing.Init, so no pod path (tracing config) is read.
func TestMainEntry_ConfigErrorExits(t *testing.T) {
	origLoad, origExit := loadConfigFn, exitFn
	t.Cleanup(func() { loadConfigFn, exitFn = origLoad, origExit })
	var exitMsg string
	exitFn = func(format string, v ...any) { exitMsg = fmt.Sprintf(format, v...) }
	loadConfigFn = func(string, func(string) string) (*config.Config, error) {
		return nil, errors.New("bad config")
	}

	main()

	if exitMsg != "failed to load config: bad config" {
		t.Fatalf("exit message = %q, want %q", exitMsg, "failed to load config: bad config")
	}
}
