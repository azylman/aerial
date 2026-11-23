package main

import (
	"context"
	"testing"

	"github.com/azylman/aerial/services/transcripts/internal/config"
)

// TestMainFunc checks main wires its package vars into httpkit.RunService;
// the bootstrap branches themselves are covered by httpkit's TestRunService.
func TestMainFunc(t *testing.T) {
	origConfigPath, origLoad, origRun, origExit := configPath, loadConfigFn, runAppFn, exitFn
	t.Cleanup(func() {
		configPath, loadConfigFn, runAppFn, exitFn = origConfigPath, origLoad, origRun, origExit
	})

	exitCalled := false
	gotPort := ""
	exitFn = func(string, ...any) { exitCalled = true }
	loadConfigFn = func(string) (*config.Config, error) { return &config.Config{Port: "8080"}, nil }
	runAppFn = func(_ context.Context, cfg *config.Config) error {
		gotPort = cfg.Port
		return nil
	}

	main()

	if gotPort != "8080" {
		t.Errorf("runAppFn cfg.Port = %q, want %q", gotPort, "8080")
	}
	if exitCalled {
		t.Error("exitFn called on clean run")
	}
}
