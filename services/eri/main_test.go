package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	_ "modernc.org/sqlite"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

func WithReloadSupplier(fn func(active *config.Config) error) ReloadOption {
	return func(o *reloadConfigOptions) {
		o.reloadSupplier = fn
	}
}

func WithSkipEnvironmentSync() ReloadOption {
	return func(o *reloadConfigOptions) {
		o.skipEnvironmentSync = true
	}
}

func WithStore(store db.Store) BrainAppOption {
	return func(o *brainAppOptions) {
		o.store = store
		o.txStore, _ = store.(db.TranscriptStore)
	}
}

func WithProcessSpawner(spawner runner.DaemonSpawner) BrainAppOption {
	return func(o *brainAppOptions) {
		o.processSpawner = spawner
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

type mockMainProcessHandle struct{ pid int }

func (m *mockMainProcessHandle) Pid() int  { return m.pid }
func (*mockMainProcessHandle) Kill() error { return nil }
func (*mockMainProcessHandle) Wait() error { return nil }

type mockMainDaemonSpawner struct {
	SpawnFn func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error)
}

func newMockMainDaemonSpawner() *mockMainDaemonSpawner { return &mockMainDaemonSpawner{} }

func serveMockCodexMain(inR io.Reader, outW io.Writer, sessID string) {
	if sessID == "" {
		sessID = "th_mock_12345"
	}
	scanner := bufio.NewScanner(inR)
	for scanner.Scan() {
		line := scanner.Text()
		var req struct {
			ID     int64  `json:"id"`
			Method string `json:"method"`
		}
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			continue
		}
		switch req.Method {
		case "initialize":
			_, _ = fmt.Fprintf(outW, "{\"jsonrpc\":\"2.0\",\"id\":%d,\"result\":{}}\n", req.ID)
		case "thread/start", "thread/resume":
			_, _ = fmt.Fprintf(outW, "{\"jsonrpc\":\"2.0\",\"id\":%d,\"result\":{\"thread\":{\"id\":%q}}}\n", req.ID, sessID)
		}
	}
}

func serveMockMuseMain(inR io.Reader, outW io.Writer, sessID string) {
	if sessID == "" {
		sessID = "018f0000-0000-7000-8000-000000000001"
	}
	scanner := bufio.NewScanner(inR)
	for scanner.Scan() {
		line := scanner.Text()
		var req struct {
			ID     int64  `json:"id"`
			Method string `json:"method"`
		}
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			continue
		}
		switch req.Method {
		case "initialize":
			_, _ = fmt.Fprintf(outW, "{\"jsonrpc\":\"2.0\",\"id\":%d,\"result\":{}}\n", req.ID)
		case "session/start", "session/resume":
			_, _ = fmt.Fprintf(outW, "{\"jsonrpc\":\"2.0\",\"id\":%d,\"result\":{\"session\":{\"sessionId\":%q}}}\n", req.ID, sessID)
		case "session/setApprovalMode":
			_, _ = fmt.Fprintf(outW, "{\"jsonrpc\":\"2.0\",\"id\":%d,\"result\":{}}\n", req.ID)
		}
	}
}

func (m *mockMainDaemonSpawner) Spawn(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
	if m != nil && m.SpawnFn != nil {
		return m.SpawnFn(ctx, cfg)
	}
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	go func() {
		defer outW.Close()
		defer errW.Close()
		if cfg.IsCodex() {
			serveMockCodexMain(inR, outW, cfg.SessionID)
			return
		}
		if cfg.IsMuse() {
			serveMockMuseMain(inR, outW, cfg.SessionID)
			return
		}
		sessID := cfg.SessionID
		if sessID == "" || !runner.IsValidUUID(sessID) {
			sessID = "550e8400-e29b-41d4-a716-446655440000"
		}
		_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", sessID)
		_, _ = io.Copy(io.Discard, inR)
	}()
	return inW, outR, errR, &mockMainProcessHandle{pid: 12345}, nil
}

func TestCreateReloadConfigFunc(t *testing.T) {
	t.Run("nil config handled", func(t *testing.T) {
		CreateReloadConfigFunc(nil)("TestNil")
	})

	t.Run("default supplier and unchanged reload", func(t *testing.T) {
		cfg := newTestConfig()
		reloadFn := CreateReloadConfigFunc(cfg, WithSkipEnvironmentSync(), WithReloadSupplier(nil))
		reloadFn("DefaultReload")
		reloadFn("UnchangedReload")
	})

	t.Run("invalid YAML with alert", func(t *testing.T) {
		s, _ := discordgo.New("Bot mock-token")
		CreateReloadConfigFunc(newTestConfig(), WithDiscordSession(s), WithReloadSupplier(func(active *config.Config) error {
			return errors.New("simulated invalid YAML error")
		}), WithSkipEnvironmentSync())("TestReload")
	})

	t.Run("supplier success updating model and marking pool dirty", func(t *testing.T) {
		s, _ := discordgo.New("Bot mock-token")
		cfg := newTestConfig(func(d *config.ConfigData) { d.Model = "gemini-2.5-pro" })
		wp := queue.New(cfg, queue.WorkerPoolConfig{})
		calledOnReload := false
		CreateReloadConfigFunc(cfg,
			WithDiscordSession(s),
			WithWorkerPool(wp),
			func(o *reloadConfigOptions) { o.onReload = func(src string) { calledOnReload = true } },
			WithReloadSupplier(func(active *config.Config) error {
				active.Update(&config.ConfigData{
					CredentialSources: config.CredentialSources{OAuth: config.CredentialSource{Disabled: true}, Codex: config.CredentialSource{Disabled: true}},
					Model:             "gemini-2.5-flash",
				})
				return nil
			}),
			WithSkipEnvironmentSync(),
		)("TestReloadSuccess")
		if !calledOnReload {
			t.Fatalf("expected onReload to be called")
		}
	})

	t.Run("complete success with sync and blocker home error", func(t *testing.T) {
		tmpDir, tmpData := t.TempDir(), t.TempDir()
		cfg := newTestConfig(func(d *config.ConfigData) {
			d.GeminiHomeDir = tmpDir
			d.DataDir = tmpData
			d.Model = "gemini-2.5-flash"
			d.APIKey = "test-api-key"
			d.SystemPrompt = "test system prompt"
		})
		reloadFn := CreateReloadConfigFunc(cfg, WithReloadSupplier(func(active *config.Config) error { return nil }))
		reloadFn("TestSource")

		blockerHome := filepath.Join(tmpDir, "blocked_home")
		if err := os.WriteFile(blockerHome, []byte("file"), 0o644); err != nil {
			t.Fatalf("failed to write blocker file: %v", err)
		}
		cfg.Update(&config.ConfigData{CredentialSources: config.CredentialSources{OAuth: config.CredentialSource{Disabled: true}, Codex: config.CredentialSource{Disabled: true}}, GeminiHomeDir: filepath.Join(blockerHome, "sub"), DataDir: tmpData})
		reloadFn("TestErrorSource")
	})
}

func TestEnsureConfigDefaults(t *testing.T) {
	t.Parallel()

	ensureConfigDefaults(nil)

	cfg := config.NewFromData(&config.ConfigData{})
	ensureConfigDefaults(cfg)
	if cfg.Current().GeminiHomeDir == "" || cfg.Current().DataDir == "" {
		t.Fatalf("expected defaults to be filled")
	}
}

func TestComputeDaemonReloadFingerprint(t *testing.T) {
	t.Parallel()

	baseCfg := newTestConfig(func(d *config.ConfigData) {
		d.Model = "gemini-3.8-flash-low"
		d.LowEffortModel = "gemini-3.8-flash-low"
		d.Engine = "gemini"
	})
	baseFP := computeDaemonReloadFingerprint(baseCfg)
	if baseFP == "" {
		t.Fatal("expected non-empty base fingerprint")
	}

	tests := []struct {
		name      string
		cfg       *config.Config
		wantDiff  bool
		wantEmpty bool
	}{
		{
			name:      "nil config produces empty fingerprint",
			cfg:       nil,
			wantEmpty: true,
		},
		{
			name: "identical config produces identical fingerprint",
			cfg: newTestConfig(func(d *config.ConfigData) {
				d.Model = "gemini-3.8-flash-low"
				d.LowEffortModel = "gemini-3.8-flash-low"
				d.Engine = "gemini"
			}),
			wantDiff: false,
		},
		{
			name: "changed model produces different fingerprint",
			cfg: newTestConfig(func(d *config.ConfigData) {
				d.Model = "gemini-2.5-pro"
				d.LowEffortModel = "gemini-3.8-flash-low"
				d.Engine = "gemini"
			}),
			wantDiff: true,
		},
		{
			name: "changed engine produces different fingerprint",
			cfg: newTestConfig(func(d *config.ConfigData) {
				d.Model = "gemini-3.8-flash-low"
				d.LowEffortModel = "gemini-3.8-flash-low"
				d.Engine = "claude"
			}),
			wantDiff: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fp := computeDaemonReloadFingerprint(tc.cfg)
			if tc.wantEmpty {
				if fp != "" {
					t.Fatalf("expected empty fingerprint, got %q", fp)
				}
				return
			}
			if tc.wantDiff {
				if fp == baseFP {
					t.Fatalf("expected different fingerprint than base, got %q", fp)
				}
			} else {
				if fp != baseFP {
					t.Fatalf("expected identical fingerprint to base, got %q (base: %q)", fp, baseFP)
				}
			}
		})
	}
}

func TestRegisterGitSyncReload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		remoteAddr string
		wantCode   int
		wantBody   string
		wantQueued bool
	}{
		{name: "ipv4 loopback post queues", method: http.MethodPost, remoteAddr: "127.0.0.1:5555", wantCode: http.StatusOK, wantBody: "{\"status\":\"queued\"}", wantQueued: true},
		{name: "ipv6 loopback post queues", method: http.MethodPost, remoteAddr: "[::1]:5555", wantCode: http.StatusOK, wantBody: "{\"status\":\"queued\"}", wantQueued: true},
		{name: "pod network rejected", method: http.MethodPost, remoteAddr: "10.42.0.9:5555", wantCode: http.StatusForbidden, wantBody: "forbidden\n"},
		{name: "get not allowed", method: http.MethodGet, remoteAddr: "127.0.0.1:5555", wantCode: http.StatusMethodNotAllowed, wantBody: "Method Not Allowed\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// No worker drains pending, so it shows whether the handler queued a reload.
			trigger := &reloadTrigger{pending: make(chan struct{}, 1)}
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
			if got := len(trigger.pending) == 1; got != tc.wantQueued {
				t.Errorf("queued = %v, want %v", got, tc.wantQueued)
			}
		})
	}
}

// blockingReloader records each reload on started and then blocks until
// release is closed.
type blockingReloader struct {
	started chan string
	release chan struct{}
	calls   atomic.Int32
}

func (b *blockingReloader) reload(source string) {
	b.calls.Add(1)
	b.started <- source
	<-b.release
}

func TestReloadTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		burst              int
		cancelDuringReload bool
		wantReloads        int32
	}{
		{name: "single trigger reloads once", burst: 0, wantReloads: 1},
		{name: "trigger during reload runs one follow-up", burst: 1, wantReloads: 2},
		{name: "burst during reload coalesces into one follow-up", burst: 5, wantReloads: 2},
		{name: "cancel during reload drops pending follow-up", burst: 3, cancelDuringReload: true, wantReloads: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rec := &blockingReloader{started: make(chan string, 8), release: make(chan struct{})}
			trigger := newReloadTrigger(ctx, rec.reload)

			trigger.Trigger()
			if got := <-rec.started; got != "git-sync webhook" {
				t.Errorf("source = %q, want %q", got, "git-sync webhook")
			}
			for range tc.burst {
				trigger.Trigger()
			}
			if tc.cancelDuringReload {
				cancel()
			}
			close(rec.release)
			for range tc.wantReloads - 1 {
				<-rec.started
			}
			cancel()
			<-trigger.done
			// With the worker stopped, Trigger must neither block nor reload.
			trigger.Trigger()

			if got := rec.calls.Load(); got != tc.wantReloads {
				t.Errorf("reloads = %d, want %d", got, tc.wantReloads)
			}
		})
	}
}

func TestDefaultPaths(t *testing.T) {
	// 1. With HOME set
	t.Setenv("HOME", "/custom/home")
	if got := DefaultGeminiHomeDir(); got != "/custom/home" {
		t.Errorf("expected /custom/home, got %s", got)
	}

	// 2. With HOME empty
	t.Setenv("HOME", "")
	gotEmpty := DefaultGeminiHomeDir()
	if gotEmpty == "" {
		t.Errorf("expected non-empty default gemini home dir")
	}

	// 3. Direct helper tests for defaultGeminiHomeDirWithLookup
	if got := defaultGeminiHomeDirWithLookup("/custom/home", false, nil); got != "/custom/home" {
		t.Errorf("expected /custom/home, got %s", got)
	}
	if got := defaultGeminiHomeDirWithLookup("", true, nil); !strings.Contains(got, "aerial-test-gemini") {
		t.Errorf("expected temp aerial-test-gemini, got %s", got)
	}
	if got := defaultGeminiHomeDirWithLookup("", false, func() (string, error) { return "/home/testuser", nil }); got != "/home/testuser" {
		t.Errorf("expected /home/testuser, got %s", got)
	}
	if got := defaultGeminiHomeDirWithLookup("", false, func() (string, error) { return "  ", nil }); got != "/root" {
		t.Errorf("expected /root for whitespace user home dir, got %s", got)
	}
	if got := defaultGeminiHomeDirWithLookup("", false, func() (string, error) { return "", errors.New("err") }); got != "/root" {
		t.Errorf("expected /root when lookup fails, got %s", got)
	}
	if got := defaultGeminiHomeDirWithLookup("", false, nil); got != "/root" {
		t.Errorf("expected /root when lookup fn is nil, got %s", got)
	}

	// 4. DefaultDataDir tests
	if got := defaultDataDir(false); got != "/data" {
		t.Errorf("expected /data from defaultDataDir(false), got %s", got)
	}
	if dir := DefaultDataDir(); dir == "" {
		t.Errorf("expected non-empty data dir, got empty")
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

	var checkOpts brainAppOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&checkOpts)
		}
	}
	var effectiveOpts []BrainAppOption
	if checkOpts.store == nil {
		effectiveOpts = append(effectiveOpts, WithStore(dbtest.NewFakeStore()))
	}
	effectiveOpts = append(effectiveOpts, opts...)

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunBrainApp(ctx, cfg, effectiveOpts...)
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

	tests := []struct {
		name       string
		mode       string
		wantErrStr string
		wantNil    bool
	}{
		{"nil config", "nil_cfg", "config cannot be nil", false},
		{"canceled context", "canceled", "", true},
		{"canceled context with init error", "canceled_init_err", "", true},
		{"missing database URL without store", "missing_db", "database URL or path is required", false},
		{"missing gemini home dir", "missing_home", "gemini home directory is required", false},
		{"missing data dir", "missing_data", "data directory is required", false},
		{"missing port", "missing_port", "port is required", false},
		{"unsupported database scheme", "unsupported_db", "failed to initialize database", false},
		{"invalid listen port", "invalid_port", "listen", false},
	}

	mkCfg := func(home, data, port, dbURL string) *config.Config {
		return newTestConfig(func(d *config.ConfigData) {
			d.GeminiHomeDir, d.DataDir, d.Port, d.DatabaseURL = home, data, port, dbURL
			d.AgyBin, d.Model, d.SystemPrompt = "/bin/true", "gemini-2.5-flash", "test"
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var cfg *config.Config
			var opts []BrainAppOption

			switch tt.mode {
			case "nil_cfg":
				cfg = nil
			case "canceled":
				cfg = mkCfg(tmpDir, filepath.Join(tmpDir, "data"), "0", ":memory:")
				cancel()
			case "canceled_init_err":
				blockDir := filepath.Join(t.TempDir(), "block")
				_ = os.WriteFile(blockDir, []byte("file"), 0o644)
				cfg = mkCfg(blockDir, filepath.Join(tmpDir, "data"), "0", ":memory:")
				cancel()
			case "missing_db":
				cfg = mkCfg(tmpDir, filepath.Join(tmpDir, "data"), "0", "")
			case "missing_home":
				cfg = mkCfg("", filepath.Join(tmpDir, "data"), "0", ":memory:")
			case "missing_data":
				cfg = mkCfg(tmpDir, "", "0", ":memory:")
			case "missing_port":
				cfg = mkCfg(tmpDir, filepath.Join(tmpDir, "data"), "", ":memory:")
			case "unsupported_db":
				cfg = mkCfg(tmpDir, filepath.Join(tmpDir, "data"), "0", "mysql://user:pass@localhost/db")
			case "invalid_port":
				cfg = mkCfg(tmpDir, filepath.Join(tmpDir, "data"), "-1", ":memory:")
				opts = []BrainAppOption{WithStore(dbtest.NewFakeStore())}
			}

			err := RunBrainApp(ctx, cfg, opts...)
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

		conflictCfg := mkCfg(tmpDir, filepath.Join(tmpDir, "data"), portStr, ":memory:")
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err = RunBrainApp(ctx, conflictCfg, WithStore(dbtest.NewFakeStore()))
		if err == nil {
			t.Fatalf("expected error from port conflict, got nil")
		}
	})
}

type mockErrProcessHandle struct{}

func (*mockErrProcessHandle) Pid() int    { return 99999 }
func (*mockErrProcessHandle) Kill() error { return errors.New("simulated kill error") }
func (*mockErrProcessHandle) Wait() error { return nil }

func newLifecycleCfg(t *testing.T, mutators ...func(*config.ConfigData)) (string, *config.Config) {
	t.Helper()
	tmpDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(tmpDir, "data", ".gemini", "config", "skills"), 0o755)
	_ = os.MkdirAll(filepath.Join(tmpDir, ".gemini", "config", "skills"), 0o755)
	cfg := newTestConfig(func(d *config.ConfigData) {
		d.Port = "0"
		d.AgyBin = "/bin/true"
		d.Model = "gemini-2.5-flash"
		d.SystemPrompt = "test"
		d.GeminiHomeDir = tmpDir
		d.DataDir = filepath.Join(tmpDir, "data")
		d.DatabaseURL = ":memory:"
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
		oldOpen := openDefaultDB
		openDefaultDB = func(*config.Config) (*sql.DB, error) {
			return sql.Open("sqlite", ":memory:")
		}
		defer func() { openDefaultDB = oldOpen }()

		_, cfg := newLifecycleCfg(t)
		_, cancel, wait := runBrainAppUntilReady(t, cfg, WithStore(nil))
		cancel()
		if err := wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("unexpected error from RunBrainApp: %v", err)
		}
	})

	t.Run("ProcessPoolInitErrors", func(t *testing.T) {
		_, cfg := newLifecycleCfg(t)
		failingSpawner := &mockMainDaemonSpawner{
			SpawnFn: func(ctx context.Context, dCfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
				return nil, nil, nil, nil, errors.New("simulated spawn failure")
			},
		}

		_, cancel, wait := runBrainAppUntilReady(t, cfg, WithProcessSpawner(failingSpawner))
		cancel()
		if err := wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("unexpected error from RunBrainApp: %v", err)
		}
	})

	t.Run("DaemonCloseErrors", func(t *testing.T) {
		_, cfg := newLifecycleCfg(t)
		spawner := &mockMainDaemonSpawner{
			SpawnFn: func(ctx context.Context, dCfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
				inR, inW := io.Pipe()
				outR, outW := io.Pipe()
				errR, errW := io.Pipe()
				_ = inR
				_ = errW
				go func() {
					_, _ = outW.Write([]byte("ready\n"))
				}()
				return inW, outR, errR, &mockErrProcessHandle{}, nil
			},
		}

		_, cancel, wait := runBrainAppUntilReady(t, cfg, WithProcessSpawner(spawner))
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
			_, _ = w.Write([]byte(`[{"name":"restate-test-quota"}]`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	t.Setenv("ERI_RESTATE_URL", ts.URL)

	_, cfg := newLifecycleCfg(t)
	client := NewRestateIngressClient("http://127.0.0.1:8080", "aerial-test")
	_, cancel, wait := runBrainAppUntilReady(t, cfg, WithRestateIngressClient(client))

	qp := currentFunnelQuotaProvider()
	if qp == nil {
		t.Fatal("expected funnel quota provider to be set by RunBrainApp with restate client")
	}
	groups := qp()
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
	store := dbtest.NewFakeStore()
	WithStore(store)(&opts)
	if opts.store != store {
		t.Errorf("expected store to be set")
	}

	spawner := newMockMainDaemonSpawner()
	WithProcessSpawner(spawner)(&opts)
	if opts.processSpawner != spawner {
		t.Errorf("expected processSpawner to be set")
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

// Global config and process environment require serial execution here.
func TestDefaultReloadCandidate(t *testing.T) {
	original := config.ActiveConfig().Current()
	t.Cleanup(func() { config.ActiveConfig().Update(original) })
	tests := []struct {
		name           string
		mutateSupplier bool
		wantModel      string
	}{
		{name: "supplier error retains lkgc", mutateSupplier: true, wantModel: "old-model"},
		{name: "successful reload updates model", mutateSupplier: false, wantModel: "new-model"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte("model: new-model\nchannels:\n  default: {mode: disabled}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("CONFIG_PATH", path)
			cfg := config.ActiveConfig()
			cfg.Update(newTestConfig(func(d *config.ConfigData) { d.Model = "old-model" }).Current())
			var opts []ReloadOption
			opts = append(opts, WithSkipEnvironmentSync())
			if tc.mutateSupplier {
				opts = append(opts, WithReloadSupplier(func(candidate *config.Config) error {
					return errors.New("forced reload failure")
				}))
			}
			CreateReloadConfigFunc(cfg, opts...)("test")
			if cfg.Current().Model != tc.wantModel {
				t.Fatalf("cfg=%q want=%q", cfg.Current().Model, tc.wantModel)
			}
		})
	}
}
