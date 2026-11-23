package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/eri/pkg/bootstrap"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

func WithStore(store db.Store) BrainAppOption {
	return func(o *brainAppOptions) {
		o.store = store
		o.txStore, _ = store.(db.TranscriptStore)
	}
}

func WithHupChannel(hupChan <-chan os.Signal) BrainAppOption {
	return func(o *brainAppOptions) {
		o.hupChan = hupChan
	}
}

func WithOnReload(fn func(source string)) BrainAppOption {
	return func(o *brainAppOptions) {
		o.onReload = fn
	}
}

// unopenedDSN is a well-formed Postgres DSN for configs whose store is
// injected or whose run ends before the database is opened.
const unopenedDSN = "postgres://eri@unopened.invalid/eri"

func newTestConfig(mutators ...func(*config.ConfigData)) *config.Config {
	data := config.DefaultConfigData()
	data.CredentialSources = config.CredentialSources{OAuth: config.CredentialSource{Disabled: true}, Codex: config.CredentialSource{Disabled: true}}
	for _, m := range mutators {
		if m != nil {
			m(data)
		}
	}
	return config.NewFromData(data)
}

func TestRegisterGitSyncReload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		remoteAddr string
		wantCode   int
		wantBody   string
	}{
		{name: "ipv4 loopback post queues", method: http.MethodPost, remoteAddr: "127.0.0.1:5555", wantCode: http.StatusOK, wantBody: "{\"status\":\"queued\"}"},
		{name: "ipv6 loopback post queues", method: http.MethodPost, remoteAddr: "[::1]:5555", wantCode: http.StatusOK, wantBody: "{\"status\":\"queued\"}"},
		{name: "pod network rejected", method: http.MethodPost, remoteAddr: "192.0.2.9:5555", wantCode: http.StatusForbidden, wantBody: "forbidden\n"},
		{name: "get not allowed", method: http.MethodGet, remoteAddr: "127.0.0.1:5555", wantCode: http.StatusMethodNotAllowed, wantBody: "Method Not Allowed\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			trigger := bootstrap.NewReloadTrigger(t.Context(), func(string) {})
			mux := http.NewServeMux()
			registerGitSyncReload(mux, trigger)
			req := httptest.NewRequest(tc.method, "/-/reload", http.NoBody)
			req.RemoteAddr = tc.remoteAddr
			w := httptest.NewRecorder()

			mux.ServeHTTP(w, req)

			if w.Code != tc.wantCode {
				t.Errorf("code = %d, want %d", w.Code, tc.wantCode)
			}
			if got := w.Body.String(); got != tc.wantBody {
				t.Errorf("body = %q, want %q", got, tc.wantBody)
			}
		})
	}
}

func runBrainAppUntilReady(t *testing.T, cfg *config.Config, opts ...BrainAppOption) (string, context.CancelFunc, func() error) {
	t.Helper()
	var readyOnce sync.Once
	ready := make(chan struct{})
	var boundAddr string
	oldReady := onServerReady
	onServerReady = func(addr string) {
		readyOnce.Do(func() {
			boundAddr = addr
			close(ready)
		})
	}
	t.Cleanup(func() {
		onServerReady = oldReady
	})

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunBrainApp(ctx, cfg, opts...)
	}()

	select {
	case <-ready:
	case err := <-errCh:
		cancel()
		t.Fatalf("RunBrainApp exited prematurely before ready: %v", err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatalf("RunBrainApp timed out waiting for server ready")
	}

	var waitCalled bool
	wait := func() error {
		waitCalled = true
		select {
		case err := <-errCh:
			return err
		case <-time.After(15 * time.Second):
			t.Errorf("RunBrainApp did not shut down within 15s")
			return errors.New("shutdown timeout")
		}
	}

	t.Cleanup(func() {
		if !waitCalled {
			cancel()
			select {
			case <-errCh:
			case <-time.After(5 * time.Second):
			}
		}
	})

	return boundAddr, cancel, wait
}

func TestRunBrainApp_ValidationAndErrors(t *testing.T) {
	tmpDir := t.TempDir()
	mkCfg := func(home, data, port, dbURL string) *config.Config {
		return newTestConfig(func(d *config.ConfigData) {
			d.GeminiHomeDir, d.DataDir, d.Port, d.DatabaseURL = home, data, port, dbURL
			d.AgyBin, d.Model, d.SystemPrompt = "/bin/true", "gemini-2.5-flash", "test"
		})
	}
	blockDir := filepath.Join(t.TempDir(), "block")
	if err := os.WriteFile(blockDir, []byte("file"), 0o644); err != nil {
		t.Fatalf("failed to write block file: %v", err)
	}
	storeOpt := []BrainAppOption{WithStore(newTestStore(t))}

	tests := []struct {
		name       string
		cfg        *config.Config
		opts       []BrainAppOption
		cancel     bool
		wantErrStr string
		wantNil    bool
	}{
		{"nil config", nil, nil, false, "config cannot be nil", false},
		{"canceled context", mkCfg(tmpDir, filepath.Join(tmpDir, "data"), "0", unopenedDSN), nil, true, "", true},
		{"canceled context with init error", mkCfg(blockDir, filepath.Join(tmpDir, "data"), "0", unopenedDSN), nil, true, "", true},
		{"missing database URL without store", mkCfg(tmpDir, filepath.Join(tmpDir, "data"), "0", ""), nil, false, "database URL or path is required", false},
		{"missing gemini home dir", mkCfg("", filepath.Join(tmpDir, "data"), "0", unopenedDSN), nil, false, "gemini home directory is required", false},
		{"missing data dir", mkCfg(tmpDir, "", "0", unopenedDSN), nil, false, "data directory is required", false},
		{"missing port", mkCfg(tmpDir, filepath.Join(tmpDir, "data"), "", unopenedDSN), nil, false, "port is required", false},
		{"unsupported database scheme", mkCfg(tmpDir, filepath.Join(tmpDir, "data"), "0", "mysql://user:pass@localhost/db"), nil, false, "failed to initialize database", false},
		{"invalid listen port", mkCfg(tmpDir, filepath.Join(tmpDir, "data"), "-1", unopenedDSN), storeOpt, false, "listen", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.cancel {
				cancel()
			}

			err := RunBrainApp(ctx, tt.cfg, tt.opts...)
			if tt.wantNil {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErrStr)
			}
			if !strings.Contains(err.Error(), tt.wantErrStr) {
				t.Errorf("expected error containing %q, got: %v", tt.wantErrStr, err)
			}
		})
	}

	t.Run("port conflict", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to bind listener: %v", err)
		}
		defer ln.Close()

		_, portStr, err := net.SplitHostPort(ln.Addr().String())
		if err != nil {
			t.Fatalf("failed to split host port: %v", err)
		}

		conflictCfg := mkCfg(tmpDir, filepath.Join(tmpDir, "data"), portStr, unopenedDSN)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err = RunBrainApp(ctx, conflictCfg, WithStore(newTestStore(t)))
		if err == nil {
			t.Fatalf("expected error from port conflict, got nil")
		}
	})
}

func newLifecycleCfg(t *testing.T, mutators ...func(*config.ConfigData)) (string, *config.Config) {
	t.Helper()
	tmpDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmpDir, "data", ".gemini", "config", "skills"), 0o755); err != nil {
		t.Fatalf("failed to mkdir skills: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(tmpDir, ".gemini", "config", "skills"), 0o755); err != nil {
		t.Fatalf("failed to mkdir skills: %v", err)
	}
	cfg := newTestConfig(func(d *config.ConfigData) {
		d.Port = "0"
		d.AgyBin = "/bin/true"
		d.Model = "gemini-2.5-flash"
		d.SystemPrompt = "test"
		d.GeminiHomeDir = tmpDir
		d.DataDir = filepath.Join(tmpDir, "data")
		d.DatabaseURL = unopenedDSN
		for _, m := range mutators {
			m(d)
		}
	})
	return tmpDir, cfg
}

func TestRunBrainApp_FullLifecycleAndPools(t *testing.T) {
	t.Run("SIGHUP", func(t *testing.T) {
		_, cfg := newLifecycleCfg(t)
		hupChan := make(chan os.Signal, 1)
		reloadReceived := make(chan string, 1)

		_, cancel, wait := runBrainAppUntilReady(t, cfg,
			WithStore(newTestStore(t)),
			WithHupChannel(hupChan),
			WithOnReload(func(src string) {
				select {
				case reloadReceived <- src:
				default:
				}
			}),
		)

		hupChan <- syscall.SIGHUP
		select {
		case src := <-reloadReceived:
			if src != "SIGHUP" {
				t.Errorf("expected reload source SIGHUP, got %q", src)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("timeout waiting for SIGHUP reload callback")
		}

		cancel()
		if err := wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("unexpected error waiting for shutdown: %v", err)
		}
	})

	t.Run("DefaultStoreSuccess", func(t *testing.T) {
		// No injected store: RunBrainApp opens the configured Postgres
		// through bootstrap.InitDefaultStores and db.New.
		_, cfg := newLifecycleCfg(t, func(d *config.ConfigData) { d.DatabaseURL = pgtest.NewDSN(t) })
		_, cancel, wait := runBrainAppUntilReady(t, cfg, WithStore(nil))
		cancel()
		if err := wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("unexpected error from RunBrainApp: %v", err)
		}
	})
}

func TestRunBrainApp_RestateIngressEnabled(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/quota" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"groups":[{"name":"restate-test-quota"}],"updated_at":"2026-11-21T10:30:00Z"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	t.Setenv("ERI_RESTATE_URL", ts.URL)

	t.Setenv("RESTATE_INGRESS_URL", "http://127.0.0.1:8080")

	_, cfg := newLifecycleCfg(t)
	_, cancel, wait := runBrainAppUntilReady(t, cfg, WithStore(newTestStore(t)))

	qp := currentFunnelQuotaProvider()
	if qp == nil {
		t.Fatal("expected funnel quota provider to be set by RunBrainApp with restate client")
	}
	groups := qp().Groups
	if len(groups) != 1 || groups[0].Name != "restate-test-quota" {
		t.Fatalf("unexpected quota groups from wired provider: %+v", groups)
	}

	cancel()
	if err := wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("unexpected error waiting for shutdown: %v", err)
	}
}

func TestBrainAppOptions_FunctionalOptionMutators(t *testing.T) {
	var opts brainAppOptions
	store := newTestStore(t)
	WithStore(store)(&opts)
	if opts.store != store {
		t.Errorf("expected store to be set")
	}

	hupChan := make(chan os.Signal, 1)
	WithHupChannel(hupChan)(&opts)
	if opts.hupChan != hupChan {
		t.Errorf("expected hupChan to be set")
	}

	called := false
	WithOnReload(func(src string) {
		called = true
	})(&opts)
	if opts.onReload == nil {
		t.Fatalf("expected onReload to be set")
	}
	opts.onReload("test")
	if !called {
		t.Errorf("expected onReload to be invoked")
	}
}
