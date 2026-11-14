package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/env"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/bwmarrin/discordgo"
	_ "modernc.org/sqlite"
)

func TestSyncBrainEnvironment(t *testing.T) {
	tests := []struct {
		name                     string
		precreateCLIConfigAsFile bool
		wantErr                  bool
	}{
		{
			name:    "success fresh and idempotent",
			wantErr: false,
		},
		{
			name:                     "conflict file in cli parent",
			precreateCLIConfigAsFile: true,
			wantErr:                  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var cfg *config.Config
			tmpDir := t.TempDir()
			if tc.precreateCLIConfigAsFile {
				conflict := filepath.Join(tmpDir, ".gemini", "antigravity-cli")
				_ = os.MkdirAll(filepath.Dir(conflict), 0o755)
				_ = os.WriteFile(conflict, []byte("file-not-dir"), 0o644)
				cfg = newTestConfig(func(d *config.ConfigData) {
					d.DataDir = tmpDir
					d.GeminiHomeDir = tmpDir
				})
			} else {
				cfg = newTestConfig(func(d *config.ConfigData) {
					d.Model = "gemini-2.5-flash"
					d.GeminiHomeDir = t.TempDir()
					d.DataDir = t.TempDir()
					d.APIKey = "test-api-key"
					d.SystemPrompt = "test prompt"
				})
			}
			provisioner := env.NewFromConfig(cfg)
			err := syncBrainEnvironment(context.Background(), provisioner, cfg)
			if (err != nil) != tc.wantErr {
				t.Errorf("wantErr=%v, got err=%v", tc.wantErr, err)
			}
			if tc.name == "success fresh and idempotent" {
				if err := syncBrainEnvironment(context.Background(), provisioner, cfg); err != nil {
					t.Fatalf("unexpected error on idempotent second call: %v", err)
				}
			}
		})
	}
}

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
	for _, m := range mutators {
		if m != nil {
			m(data)
		}
	}
	return config.NewFromData(data)
}

type mockMainProcessHandle struct{ pid int }

func (m *mockMainProcessHandle) Pid() int    { return m.pid }
func (m *mockMainProcessHandle) Kill() error { return nil }
func (m *mockMainProcessHandle) Wait() error { return nil }

type mockMainDaemonSpawner struct {
	SpawnFn func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error)
}

func newMockMainDaemonSpawner() *mockMainDaemonSpawner { return &mockMainDaemonSpawner{} }

func (m *mockMainDaemonSpawner) Spawn(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
	if m != nil && m.SpawnFn != nil {
		return m.SpawnFn(ctx, cfg)
	}
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	go func() {
		sessID := cfg.SessionID
		if sessID == "" || !runner.IsValidUUID(sessID) {
			sessID = "550e8400-e29b-41d4-a716-446655440000"
		}
		_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":%q}\n", sessID)
		_ = errW.Close()
		_, _ = io.Copy(io.Discard, inR)
	}()
	return inW, outR, errR, &mockMainProcessHandle{pid: 12345}, nil
}

func TestCreateReloadConfigFunc(t *testing.T) {
	t.Run("invalid YAML with alert", func(t *testing.T) {
		s, _ := discordgo.New("Bot mock-token")
		CreateReloadConfigFunc(newTestConfig(), WithDiscordSession(s), WithReloadSupplier(func(active *config.Config) error {
			return errors.New("simulated invalid YAML error")
		}), WithSkipEnvironmentSync())("TestReload")
	})

	t.Run("supplier success updating model", func(t *testing.T) {
		s, _ := discordgo.New("Bot mock-token")
		CreateReloadConfigFunc(newTestConfig(), WithDiscordSession(s), WithReloadSupplier(func(active *config.Config) error {
			active.Update(&config.ConfigData{Model: "gemini-2.5-flash"})
			return nil
		}), WithSkipEnvironmentSync())("TestReloadSuccess")
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
		cfg.Update(&config.ConfigData{GeminiHomeDir: filepath.Join(blockerHome, "sub"), DataDir: tmpData})
		reloadFn("TestErrorSource")
	})

	t.Run("provisioner error", func(t *testing.T) {
		cfg := newTestConfig()
		tmpFile := filepath.Join(t.TempDir(), "blocking_file")
		_ = os.WriteFile(tmpFile, []byte("file"), 0o644)
		cfg.Update(&config.ConfigData{GeminiHomeDir: tmpFile})
		CreateReloadConfigFunc(cfg,
			WithReloadSupplier(func(active *config.Config) error { return nil }),
			WithProvisioner(env.NewFromConfig(cfg)),
		)("test-provisioner-error")
	})

	t.Run("with worker pool and claude engine propagation", func(t *testing.T) {
		cfg := newTestConfig(func(d *config.ConfigData) {
			d.LowEffortModel = "gpt-oss-120b-medium"
		})
		var spawnedModel, spawnedEngine, spawnedClaudeBin string
		spawner := &mockMainDaemonSpawner{
			SpawnFn: func(ctx context.Context, dCfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
				spawnedModel = dCfg.Model
				spawnedEngine = dCfg.Engine
				spawnedClaudeBin = dCfg.ClaudeBin
				return newMockMainDaemonSpawner().Spawn(ctx, dCfg)
			},
		}
		ephemPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, spawner)
		defer func() { _ = ephemPool.Close() }()

		var workerSpawnedEngine, workerSpawnedClaudeBin string
		workerSpawner := &mockMainDaemonSpawner{
			SpawnFn: func(ctx context.Context, dCfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
				workerSpawnedEngine = dCfg.Engine
				workerSpawnedClaudeBin = dCfg.ClaudeBin
				return newMockMainDaemonSpawner().Spawn(ctx, dCfg)
			},
		}
		mockDiscordPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, workerSpawner)
		defer func() { _ = mockDiscordPool.Close() }()
		wp := queue.New(cfg, queue.WorkerPoolConfig{
			Pools: map[queue.PoolKey]runner.AgentPool{
				{Ephemeral: false, Effort: "high"}: mockDiscordPool,
			},
		})

		CreateReloadConfigFunc(cfg,
			WithSkipEnvironmentSync(),
			WithReloadSupplier(func(active *config.Config) error {
				active.Update(&config.ConfigData{
					Engine:         "claude",
					ClaudeBin:      "/opt/anthropic/bin/claude",
					LowEffortModel: "gpt-oss-120b-medium",
				})
				return nil
			}),
			WithWorkerPool(wp),
			WithEphemeralPool(ephemPool),
		)("TestWithWorkerPool")

		_, _ = ephemPool.GetOrCreate(context.Background(), "ephemeral:worker-0", "")
		if spawnedModel != "gpt-oss-120b-medium" {
			t.Fatalf("expected ephemeralPool model updated to gpt-oss-120b-medium, got %q", spawnedModel)
		}
		if spawnedEngine != "claude" || spawnedClaudeBin != "/opt/anthropic/bin/claude" {
			t.Fatalf("expected ephemeralPool engine claude and bin /opt/anthropic/bin/claude, got %q, %q", spawnedEngine, spawnedClaudeBin)
		}

		_, _ = mockDiscordPool.GetOrCreate(context.Background(), "discord:worker-0", "")
		if workerSpawnedEngine != "claude" || workerSpawnedClaudeBin != "/opt/anthropic/bin/claude" {
			t.Fatalf("expected worker pool engine claude and bin /opt/anthropic/bin/claude, got %q, %q", workerSpawnedEngine, workerSpawnedClaudeBin)
		}
	})
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
			req := httptest.NewRequest(tc.method, "/-/reload", nil)
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

	// 5. resolveBrainModelDefaults tests
	lowD, medD := resolveBrainModelDefaults(&config.ConfigData{})
	if lowD != "gemini-3.8-flash-low" || medD != "gemini-3.8-flash-medium" {
		t.Errorf("expected default models, got %s, %s", lowD, medD)
	}
	lowC, medC := resolveBrainModelDefaults(&config.ConfigData{LowEffortModel: "custom-low", MediumEffortModel: "custom-med"})
	if lowC != "custom-low" || medC != "custom-med" {
		t.Errorf("expected custom models, got %s, %s", lowC, medC)
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
	if checkOpts.processSpawner == nil {
		effectiveOpts = append(effectiveOpts, WithProcessSpawner(newMockMainDaemonSpawner()))
	}
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

func (m *mockErrProcessHandle) Pid() int    { return 99999 }
func (m *mockErrProcessHandle) Kill() error { return errors.New("simulated kill error") }
func (m *mockErrProcessHandle) Wait() error { return nil }

func newTrackingSpawner(mu *sync.Mutex, seen map[string]runner.DaemonConfig, allSpawned chan struct{}) *mockMainDaemonSpawner {
	var once sync.Once
	baseMock := newMockMainDaemonSpawner()
	return &mockMainDaemonSpawner{
		SpawnFn: func(ctx context.Context, dCfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			mu.Lock()
			seen[dCfg.ThreadID] = dCfg
			if seen["ephemeral:worker-0"].ThreadID != "" && seen["ephemeral:worker-1"].ThreadID != "" &&
				seen["discord:worker-0"].ThreadID != "" && seen["discord:worker-1"].ThreadID != "" &&
				seen["discord:medium-worker-0"].ThreadID != "" && seen["discord:medium-worker-1"].ThreadID != "" {
				once.Do(func() { close(allSpawned) })
			}
			mu.Unlock()
			return baseMock.Spawn(ctx, dCfg)
		},
	}
}

func runBrainAppLifecyclePrewarmedWorkers(t *testing.T, cfg *config.Config, expectedEphemeralHome, expectedDiscordHome string) {
	t.Helper()
	var mu sync.Mutex
	seen := make(map[string]runner.DaemonConfig)
	allSpawned := make(chan struct{})
	trackingSpawner := newTrackingSpawner(&mu, seen, allSpawned)

	boundAddr, cancel, wait := runBrainAppUntilReady(t, cfg, WithProcessSpawner(trackingSpawner))
	resp, err := http.Post(fmt.Sprintf("http://%s/prompt", boundAddr), "application/json", strings.NewReader(`{"prompt":"Hello test prompt","channel_id":"1542423172400291873","message_id":"msg-1"}`))
	if err == nil {
		_ = resp.Body.Close()
	}

	select {
	case <-allSpawned:
	case <-time.After(2 * time.Second):
	}

	cancel()
	if err := wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("unexpected error waiting for server shutdown: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, target := range []string{"ephemeral:worker-0", "ephemeral:worker-1"} {
		if !hasEnv(seen[target].Env, "HOME="+expectedEphemeralHome) {
			t.Errorf("%s expected HOME=%q, got %v", target, expectedEphemeralHome, seen[target].Env)
		}
		if !hasEnv(seen[target].Env, "AERIAL_BOT_ID=aerial-main") {
			t.Errorf("%s expected AERIAL_BOT_ID=aerial-main, got %v", target, seen[target].Env)
		}
		if !hasEnv(seen[target].Env, "SYNAPSE_URL=http://synapse.local:4020") {
			t.Errorf("%s expected SYNAPSE_URL=http://synapse.local:4020, got %v", target, seen[target].Env)
		}
		if !hasEnv(seen[target].Env, "VANADIS_URL=http://vanadis.local:4025") {
			t.Errorf("%s expected VANADIS_URL=http://vanadis.local:4025, got %v", target, seen[target].Env)
		}
		if seen[target].MemoryRetriever != nil {
			t.Errorf("%s expected nil MemoryRetriever, got non-nil", target)
		}
	}
	for _, target := range []string{"discord:worker-0", "discord:worker-1", "discord:medium-worker-0", "discord:medium-worker-1"} {
		if !hasEnv(seen[target].Env, "HOME="+expectedDiscordHome) {
			t.Errorf("%s expected HOME=%q, got %v", target, expectedDiscordHome, seen[target].Env)
		}
		if seen[target].MemoryRetriever == nil {
			t.Errorf("%s expected non-nil MemoryRetriever, got nil", target)
		}
	}
	if seen["discord:medium-worker-0"].ThreadID == "" || seen["discord:medium-worker-1"].ThreadID == "" {
		t.Errorf("medium workers not spawned: %+v", seen)
	}
}

func runBrainAppLifecycleClaudeEngine(t *testing.T, cfg *config.Config) {
	t.Helper()
	var mu sync.Mutex
	seen := make(map[string]runner.DaemonConfig)
	allSpawned := make(chan struct{})
	trackingSpawner := newTrackingSpawner(&mu, seen, allSpawned)

	boundAddr, cancel, wait := runBrainAppUntilReady(t, cfg, WithProcessSpawner(trackingSpawner))
	resp, err := http.Post(fmt.Sprintf("http://%s/prompt", boundAddr), "application/json", strings.NewReader(`{"prompt":"Hello test prompt","channel_id":"1542423172400291873","message_id":"msg-1"}`))
	if err == nil {
		_ = resp.Body.Close()
	}

	select {
	case <-allSpawned:
	case <-time.After(2 * time.Second):
	}

	cancel()
	if err := wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("unexpected error waiting for server shutdown: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, target := range []string{"ephemeral:worker-0", "ephemeral:worker-1", "discord:worker-0", "discord:worker-1", "discord:medium-worker-0", "discord:medium-worker-1"} {
		if seen[target].Engine != "claude" {
			t.Errorf("%s expected Engine=claude, got %q", target, seen[target].Engine)
		}
		if seen[target].ClaudeBin != "/usr/local/bin/claude" {
			t.Errorf("%s expected ClaudeBin=/usr/local/bin/claude, got %q", target, seen[target].ClaudeBin)
		}
	}
}

func TestRunBrainApp_FullLifecycleAndPools(t *testing.T) {
	newLifecycleCfg := func(t *testing.T, mutators ...func(*config.ConfigData)) (string, *config.Config) {
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

	t.Run("PrewarmedWorkers", func(t *testing.T) {
		tmpDir, cfg := newLifecycleCfg(t, func(d *config.ConfigData) {
			d.DiscordToken = "mock-discord-token"
			d.HarnessAPIKey = "gemini-test-key"
			d.DataDir = d.GeminiHomeDir
			d.LowEffortModel = "gemini-test-low"
			d.MediumEffortModel = "gemini-test-medium"
			d.Ollama.BaseURL = "http://127.0.0.1:65534"
			d.BotID = "aerial-main"
			d.SynapseURL = "http://synapse.local:4020"
			d.VanadisURL = "http://vanadis.local:4025"
			d.Voice = config.VoiceConfig{
				Engine:           "gemini_api",
				Model:            "gemini-2.5-flash",
				PrewarmedTargets: []string{"kiosk"},
				AmbientContext:   &config.AmbientContextConfig{Template: "Time: {{.now}}", CacheTTL: "5s"},
			}
			d.Channels = map[string]config.ChannelPolicy{
				"default": {Mode: "threads", AmbientContext: &config.AmbientContextConfig{Template: "Time: {{.now}}", CacheTTL: "5s"}},
			}
		})

		runBrainAppLifecyclePrewarmedWorkers(t, cfg, filepath.Join(tmpDir, "runtimes", "ephemeral"), filepath.Join(tmpDir, "runtimes", "discord"))
	})

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

	t.Run("ClaudeEngineFullLifecycle", func(t *testing.T) {
		_, cfg := newLifecycleCfg(t, func(d *config.ConfigData) {
			d.Engine = "claude"
			d.ClaudeBin = "/usr/local/bin/claude"
			d.DiscordToken = "mock-discord-token"
			d.DataDir = d.GeminiHomeDir
			d.LowEffortModel = "claude-haiku"
			d.MediumEffortModel = "claude-sonnet"
			d.Voice = config.VoiceConfig{
				Engine: "agy",
				Model:  "claude-voice",
			}
		})

		runBrainAppLifecycleClaudeEngine(t, cfg)
	})
}

func hasEnv(envs []string, target string) bool {
	return slices.Contains(envs, target)
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
