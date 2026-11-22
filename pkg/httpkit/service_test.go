package httpkit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
)

func TestRunService(t *testing.T) {
	missingTracing := filepath.Join(t.TempDir(), "missing-tracing.yaml")
	tests := []struct {
		name     string
		loadErr  error
		runErr   error
		wantRun  bool
		wantExit string
	}{
		{name: "config load error exits before run", loadErr: errors.New("bad yaml"), wantExit: "Fatal configuration error: bad yaml"},
		{name: "run error exits", runErr: errors.New("listen failed"), wantRun: true, wantExit: "Server failure: listen failed"},
		{name: "server closed does not exit", runErr: http.ErrServerClosed, wantRun: true},
		{name: "clean return does not exit", wantRun: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath, gotCfg, gotExit string
			ran := false
			RunService(ServiceSpec[string]{
				Name:              "svc-test",
				ConfigPath:        "/cfg/svc.yaml",
				TracingConfigPath: missingTracing,
				Load: func(path string) (string, error) {
					gotPath = path
					return "loaded", tc.loadErr
				},
				Run: func(ctx context.Context, cfg string) error {
					ran = true
					gotCfg = cfg
					return tc.runErr
				},
				Exit: func(format string, args ...any) { gotExit = fmt.Sprintf(format, args...) },
			})
			if gotPath != "/cfg/svc.yaml" {
				t.Errorf("Load path = %q, want %q", gotPath, "/cfg/svc.yaml")
			}
			if ran != tc.wantRun {
				t.Errorf("ran = %v, want %v", ran, tc.wantRun)
			}
			if tc.wantRun && gotCfg != "loaded" {
				t.Errorf("Run cfg = %q, want %q", gotCfg, "loaded")
			}
			if gotExit != tc.wantExit {
				t.Errorf("exit = %q, want %q", gotExit, tc.wantExit)
			}
		})
	}
}
