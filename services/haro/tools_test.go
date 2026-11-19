package main

import (
	"context"
	"database/sql"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMain_Execution(t *testing.T) {
	oldRun, oldExit, oldPath := runAppFn, exitFn, configPath
	t.Cleanup(installTestTracing)
	defer func() {
		runAppFn, exitFn, configPath = oldRun, oldExit, oldPath
	}()

	tmpDir := t.TempDir()
	testCfg := filepath.Join(tmpDir, "config.yaml")
	_ = os.WriteFile(testCfg, []byte("port: \"4005\"\ntimezone: \"America/Los_Angeles\"\neri_url: \"http://127.0.0.1:59484\"\nvanadis_url: \"http://127.0.0.1:59485\"\n"), 0o644)
	configPath = testCfg
	t.Setenv("CONFIG_PATH", testCfg)
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")

	runCalled := false
	runAppFn = func(ctx context.Context, cfg *Config) error {
		runCalled = true
		return nil
	}
	main()
	if !runCalled {
		t.Error("expected runAppFn called")
	}

	exitCalled := false
	exitFn = func(string, ...any) { exitCalled = true }
	runAppFn = func(ctx context.Context, cfg *Config) error { return http.ErrServerClosed }
	main()
	if exitCalled {
		t.Error("did not expect exitFn on http.ErrServerClosed")
	}

	runAppFn = func(ctx context.Context, cfg *Config) error { return context.DeadlineExceeded }
	main()
	if !exitCalled {
		t.Error("expected exitFn on server error")
	}

	exitCalled = false
	configPath = filepath.Join(tmpDir, "nonexistent.yaml")
	t.Setenv("CONFIG_PATH", configPath)
	main()
	if !exitCalled {
		t.Error("expected exitFn on config load error")
	}
}

func TestRunApp_Lifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tempDB := filepath.Join(t.TempDir(), "runapp.db")

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunApp(ctx, &Config{
			Port:        "59483",
			DatabaseURL: tempDB,
			EriURL:      "http://127.0.0.1:59484",
			VanadisURL:  "http://127.0.0.1:59485",
		})
	}()

	var resp *http.Response
	for start := time.Now(); time.Since(start) < 3*time.Second; {
		var err error
		if resp, err = http.Get("http://127.0.0.1:59483/health"); err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		<-time.After(20 * time.Millisecond)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("health check failed, status: %v", resp)
	}
	_ = resp.Body.Close()

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("unexpected error from RunApp shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for RunApp shutdown")
	}

	origOpenDBFn := openDBFn
	openDBFn = func(ctx context.Context, cfg *Config) (*sql.DB, error) {
		if cfg != nil && strings.HasPrefix(strings.TrimSpace(cfg.DatabaseURL), "postgres://") {
			return nil, context.DeadlineExceeded
		}
		return origOpenDBFn(ctx, cfg)
	}
	defer func() { openDBFn = origOpenDBFn }()

	for _, cfg := range []*Config{
		{Port: "9999", DatabaseURL: "postgres://invalid:pass@127.0.0.1:59996/db?sslmode=disable", EriURL: "http://127.0.0.1:59484"},
		nil,
		{Port: "", DatabaseURL: ":memory:", EriURL: "http://127.0.0.1:59484"},
		{Port: "-1", DatabaseURL: ":memory:", EriURL: "http://127.0.0.1:59484"},
		{Port: "8080", DatabaseURL: ":memory:", EriURL: ""},
	} {
		if err := RunApp(context.Background(), cfg); err == nil {
			t.Errorf("expected error running app with %v", cfg)
		}
	}
}

func TestNewBusFn_FastFailure(t *testing.T) {
	if _, err := newBusFn(context.Background(), "://bad-url"); err == nil {
		t.Error("expected error connecting bus to malformed url")
	}
}
