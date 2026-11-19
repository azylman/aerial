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

	"github.com/azylman/aerial/services/haro/internal/config"
	"github.com/azylman/aerial/services/haro/internal/server"
	"github.com/azylman/aerial/services/haro/internal/testutil"
	"github.com/azylman/aerial/services/haro/internal/ticker"
)

// TestRoot_InternalDelegates exercises the internal packages main.go wires
// directly (config, ticker, server) from the root test package.
func TestRoot_InternalDelegates(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	_ = os.WriteFile(cfgPath, []byte("port: \"4005\"\ntimezone: \"America/Los_Angeles\"\neri_url: \"http://127.0.0.1:59484\"\nvanadis_url: \"http://127.0.0.1:59485\"\n"), 0o644)
	t.Setenv("CONFIG_PATH", filepath.Join(tmpDir, "resolve.yaml"))
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")

	tests := []struct {
		name    string
		call    string
		wantErr string
		wantOut string
	}{
		{"resolve config path", "resolve", "", filepath.Join(tmpDir, "resolve.yaml")},
		{"load config", "load", "", "4005"},
		{"register tokens", "register", "", ""},
		{"new ticker", "ticker", "", ""},
		{"one-shot nil message", "oneshot-nil", "message cannot be nil", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			switch tc.call {
			case "resolve":
				if got := config.ResolveConfigPath("fallback"); got != tc.wantOut {
					t.Errorf("config.ResolveConfigPath() = %q, want %q", got, tc.wantOut)
				}
			case "load":
				cfg, err := config.LoadConfig(cfgPath)
				if err != nil {
					t.Fatalf("config.LoadConfig() err = %v", err)
				}
				if cfg.Port != tc.wantOut {
					t.Errorf("config.LoadConfig().Port = %q, want %q", cfg.Port, tc.wantOut)
				}
			case "register":
				config.RegisterConfigTokens(&Config{Port: "4005", DatabaseURL: "postgres://user:pass@localhost:5432/db"})
			case "ticker":
				if got := ticker.NewTicker(nil, nil, time.Second); got == nil {
					t.Error("ticker.NewTicker() = nil, want non-nil ticker")
				}
			default:
				err := server.HandleOneShotJob(t.Context(), nil, nil, nil)
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("server.HandleOneShotJob() err = %v, want containing %q", err, tc.wantErr)
				}
			}
		})
	}
}

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
	runAppFn = func(ctx context.Context, cfg *config.Config) error {
		runCalled = true
		return nil
	}
	main()
	if !runCalled {
		t.Error("expected runAppFn called")
	}

	exitCalled := false
	exitFn = func(string, ...any) { exitCalled = true }
	runAppFn = func(ctx context.Context, cfg *config.Config) error { return http.ErrServerClosed }
	main()
	if exitCalled {
		t.Error("did not expect exitFn on http.ErrServerClosed")
	}

	runAppFn = func(ctx context.Context, cfg *config.Config) error { return context.DeadlineExceeded }
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
	origOpenDBFn := openDBFn
	openDBFn = func(ctx context.Context, cfg *config.Config) (*sql.DB, error) {
		if cfg == nil {
			return nil, context.DeadlineExceeded
		}
		return testutil.OpenSQLiteTestDB(cfg.DatabaseURL) //nolint:contextcheck // test sqlite opener takes no ctx (frozen testutil seam); wrapper must match the openDBFn signature.
	}
	defer func() { openDBFn = origOpenDBFn }()

	ctx, cancel := context.WithCancel(context.Background())
	tempDB := filepath.Join(t.TempDir(), "runapp.db")

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunApp(ctx, &config.Config{
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

	realOpenDBFn := openDBFn
	openDBFn = func(ctx context.Context, cfg *config.Config) (*sql.DB, error) {
		if cfg != nil && strings.HasPrefix(strings.TrimSpace(cfg.DatabaseURL), "postgres://") {
			return nil, context.DeadlineExceeded
		}
		return realOpenDBFn(ctx, cfg)
	}
	defer func() { openDBFn = origOpenDBFn }()

	for _, cfg := range []*config.Config{
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
