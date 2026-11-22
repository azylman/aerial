package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/vanadis-restate/internal/app"
)

func TestMain_Execution(t *testing.T) {
	origLoad, origRun, origExit, origTracing, origRead := loadConfigFn, runAppFn, exitFn, initTracingFn, readFileFn
	t.Cleanup(func() {
		loadConfigFn, runAppFn, exitFn, initTracingFn, readFileFn = origLoad, origRun, origExit, origTracing, origRead
	})
	readFileFn = func(string) ([]byte, error) { return []byte("vanadis_restate:\n  github_owner: acme\n"), nil }
	var tracedService string
	flushes := 0
	initTracingFn = func(_ context.Context, service, _ string) (tracing.Config, func()) {
		tracedService = service
		return tracing.Config{}, func() { flushes++ }
	}

	loadConfigFn = func(func(string) string) app.Config {
		return app.Config{HealthPort: "4026", RestatePort: "9080", VanadisURL: "http://vanadis.aerial:4025"}
	}

	// 1. Success case: the owner read at startup reaches RunApp
	var gotOwner string
	runAppFn = func(ctx context.Context, cfg app.Config) error {
		gotOwner = cfg.GitHubOwner
		return nil
	}
	exited := false
	exitFn = func(format string, v ...any) {
		exited = true
	}
	main()
	assert.False(t, exited)
	assert.Equal(t, "vanadis-restate", tracedService)
	assert.Equal(t, 1, flushes)
	assert.Equal(t, "acme", gotOwner)

	// 2. Error case
	runAppFn = func(ctx context.Context, cfg app.Config) error {
		return errors.New("startup failed")
	}
	main()
	assert.True(t, exited)

	// 3. Missing github_owner exits before RunApp
	exited = false
	ran := false
	runAppFn = func(context.Context, app.Config) error {
		ran = true
		return nil
	}
	readFileFn = func(string) ([]byte, error) { return []byte("vanadis_restate: {}\n"), nil }
	main()
	assert.True(t, exited)
	assert.False(t, ran)
}
