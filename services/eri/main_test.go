package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func TestInitializeBrainEnvironment(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T) *config.Config
		wantErr bool
	}{
		{
			name:    "nil config",
			setup:   func(t *testing.T) *config.Config { return nil },
			wantErr: false,
		},
		{
			name: "success fresh and idempotent",
			setup: func(t *testing.T) *config.Config {
				return newTestConfig(func(d *config.ConfigData) {
					d.Model = "gemini-2.5-flash"
					d.GeminiHomeDir = t.TempDir()
					d.DataDir = t.TempDir()
					d.APIKey = "test-api-key"
					d.SystemPrompt = "test prompt"
				})
			},
			wantErr: false,
		},
		{
			name: "conflict file in cli parent",
			setup: func(t *testing.T) *config.Config {
				tmpDir := t.TempDir()
				conflict := filepath.Join(tmpDir, ".gemini", "antigravity-cli")
				_ = os.MkdirAll(filepath.Dir(conflict), 0o755)
				_ = os.WriteFile(conflict, []byte("file-not-dir"), 0o644)
				return newTestConfig(func(d *config.ConfigData) {
					d.DataDir = tmpDir
					d.GeminiHomeDir = tmpDir
				})
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.setup(t)
			err := InitializeBrainEnvironment(context.Background(), cfg)
			if (err != nil) != tc.wantErr {
				t.Errorf("wantErr=%v, got err=%v", tc.wantErr, err)
			}
			if tc.name == "success fresh and idempotent" {
				if err := InitializeBrainEnvironment(context.Background(), cfg); err != nil {
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
		CreateReloadConfigFunc(cfg, WithProvisioner(env.NewFromConfig(cfg)))("test-provisioner-error")
	})

	t.Run("with worker pool", func(t *testing.T) {
		cfg := newTestConfig()
		CreateReloadConfigFunc(cfg, WithSkipEnvironmentSync(), WithReloadSupplier(func(active *config.Config) error { return nil }), WithWorkerPool(queue.New(cfg, queue.WorkerPoolConfig{})))("TestWithWorkerPool")
	})
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

		expectedEphemeralHome := filepath.Join(tmpDir, "runtimes", "ephemeral")
		expectedDiscordHome := filepath.Join(tmpDir, "runtimes", "discord")

		var mu sync.Mutex
		seen := make(map[string]runner.DaemonConfig)
		allSpawned := make(chan struct{})
		var once sync.Once
		baseMock := newMockMainDaemonSpawner()

		trackingSpawner := &mockMainDaemonSpawner{
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
}

func hasEnv(envs []string, target string) bool {
	for _, e := range envs {
		if e == target {
			return true
		}
	}
	return false
}

func TestBrainAppOptions_Coverage(t *testing.T) {
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

func TestVoicePool_FactoryWiring(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	voiceHome := filepath.Join(tmpDir, "voice")
	spawner := newMockMainDaemonSpawner()

	mkCfg := func(engine, model, harnessKey, apiKey, tmpl string) *config.Config {
		cd := &config.ConfigData{
			HarnessAPIKey: harnessKey,
			APIKey:        apiKey,
			SystemPrompt:  "Test System Prompt",
			Voice: config.VoiceConfig{
				Engine:           engine,
				Model:            model,
				PrewarmedTargets: []string{"kiosk"},
			},
		}
		if tmpl != "" {
			cd.Voice.AmbientContext = &config.AmbientContextConfig{
				CacheTTL: "30s",
				Template: tmpl,
				Tools:    []config.AmbientToolConfig{{Name: "ha", Tool: "get_state"}},
			}
		}
		return config.NewFromData(cd)
	}

	tests := []struct {
		name          string
		cfg           *config.Config
		wantGeminiAPI bool
	}{
		{"GeminiAPI_WithHarnessAPIKey", mkCfg("gemini_api", "gemini-2.5-flash", "test-harness-key", "", ""), true},
		{"GeminiAPI_WithAmbientContext", mkCfg("gemini_api", "", "", "test-ambient-key", "Status: {{.ha.state}}"), true},
		{"GeminiAPI_WithAmbientContext_InvalidTemplate", mkCfg("gemini_api", "", "", "test-ambient-key", "{{.invalid"), true},
		{"GeminiAPI_WithAPIKeyFallback", mkCfg("gemini_api", "", "", "test-standard-key", ""), true},
		{"GeminiAPI_EmptyKeyFallbackToAgy", mkCfg("gemini_api", "", "", "", ""), false},
		{"AgyEngine", mkCfg("agy", "", "some-key", "", ""), false},
		{"NilConfig", nil, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := createVoiceProcessPool(tc.cfg, voiceHome, "fallback-model", spawner)
			defer pool.Close()

			dynPool, ok := pool.(*runner.DynamicVoicePool)
			if !ok {
				t.Fatalf("expected *runner.DynamicVoicePool, got %T", pool)
			}
			lease, lErr := dynPool.AcquireLease(context.Background(), "k")
			if lErr != nil {
				t.Fatalf("AcquireLease failed: %v", lErr)
			}
			isGemini := lease.SessionID() == "k"
			_ = lease.Release()
			if isGemini != tc.wantGeminiAPI {
				t.Fatalf("expected GeminiAPISession=%v, got sessionID=%q", tc.wantGeminiAPI, lease.SessionID())
			}
		})
	}

	t.Run("DynamicHotReload_EngineSwitch", func(t *testing.T) {
		t.Parallel()
		data := &config.ConfigData{
			HarnessAPIKey: "harness-key",
			Voice: config.VoiceConfig{
				Engine: "agy",
				Model:  "agy-model",
			},
		}
		cfg := config.NewFromData(data)

		pool := createVoiceProcessPool(cfg, voiceHome, "fallback-model", spawner)
		defer pool.Close()

		dynPool, ok := pool.(*runner.DynamicVoicePool)
		if !ok {
			t.Fatalf("expected *runner.DynamicVoicePool, got %T", pool)
		}
		if lease, err := dynPool.AcquireLease(context.Background(), "k"); err != nil {
			t.Fatalf("AcquireLease failed: %v", err)
		} else {
			sid := lease.SessionID()
			_ = lease.Release()
			if sid == "k" {
				t.Fatalf("expected initial pool to be UnifiedProcessPool, got sessionID=%q", sid)
			}
		}

		// Update config to gemini_api
		cfg.Update(&config.ConfigData{
			HarnessAPIKey: "harness-key",
			Voice: config.VoiceConfig{
				Engine: "gemini_api",
				Model:  "gemini-2.5-pro",
			},
		})

		// Trigger MarkDirty on pool
		dynPool.MarkDirty()

		// Verify pool swapped to GeminiAPIPool
		if lease, err := dynPool.AcquireLease(context.Background(), "k"); err != nil {
			t.Fatalf("AcquireLease failed: %v", err)
		} else {
			sid := lease.SessionID()
			_ = lease.Release()
			if sid != "k" {
				t.Fatalf("expected swapped pool to be GeminiAPIPool, got sessionID=%q", sid)
			}
		}
	})
}

func TestExtractMCPServers(t *testing.T) {
	t.Parallel()

	// 1. Nil config
	nilVoice := extractMCPServers(nil, false)
	if len(nilVoice) != 1 || nilVoice[0].Name != "scheduler" || nilVoice[0].ServerURL != "http://haro.aerial/mcp" {
		t.Fatalf("expected scheduler default for nil voice config, got: %+v", nilVoice)
	}
	nilAll := extractMCPServers(nil, true)
	if len(nilAll) != 1 || nilAll[0].Name != "scheduler" || nilAll[0].ServerURL != "http://haro.aerial/mcp" {
		t.Fatalf("expected scheduler default for nil all config, got: %+v", nilAll)
	}

	// 2. Populated config with Common, Voice, Discord
	rawValid := json.RawMessage(`{"serverUrl":"http://alpha:8080/mcp","headers":{"Auth":"Bearer tok"}}`)
	rawDiscord := json.RawMessage(`{"serverUrl":"http://discord:8080/mcp"}`)
	rawVoice := json.RawMessage(`{"serverUrl":"http://voice:8080/mcp"}`)
	data := &config.ConfigData{
		McpServers: config.TargetMcpConfig{
			Common: map[string]json.RawMessage{
				"alpha": rawValid,
			},
			Discord: map[string]json.RawMessage{
				"beta": rawDiscord,
			},
			Voice: map[string]json.RawMessage{
				"gamma": rawVoice,
			},
		},
	}

	voiceServers := extractMCPServers(data, false)
	if len(voiceServers) != 3 {
		t.Fatalf("expected 3 servers for voice (scheduler, alpha, gamma), got %d: %+v", len(voiceServers), voiceServers)
	}
	allServers := extractMCPServers(data, true)
	if len(allServers) != 4 {
		t.Fatalf("expected 4 servers for all (scheduler, alpha, beta, gamma), got %d: %+v", len(allServers), allServers)
	}
}

func envSliceToMap(env []string) map[string]string {
	m := make(map[string]string, len(env))
	for _, e := range env {
		if k, v, ok := strings.Cut(e, "="); ok {
			m[k] = v
		}
	}
	return m
}

func TestBuildPoolEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		base      []string
		useTmpDir bool
		tz        []string
		wantExact []string
		wantEnv   map[string]string
		checkDirs bool
	}{
		{"EmptyRuntimeBaseReturnsBaseEnv", []string{"FOO=bar", "BAZ=qux"}, false, nil, []string{"FOO=bar", "BAZ=qux"}, nil, false},
		{"InjectsUnifiedCacheDirectories", []string{"EXISTING=1"}, true, nil, nil, map[string]string{"GOCACHE": "<tmp>/cache/go-build", "GOPATH": "<tmp>/cache/go", "GOMODCACHE": "<tmp>/cache/go/pkg/mod", "GOLANGCI_LINT_CACHE": "<tmp>/cache/golangci-lint", "EXISTING": "1"}, true},
		{"PreservesExplicitCacheEnv", []string{"GOCACHE=/custom/gocache", "GOPATH=/custom/gopath", "GOMODCACHE=/custom/modcache", "GOLANGCI_LINT_CACHE=/custom/lintcache"}, true, nil, nil, map[string]string{"GOCACHE": "/custom/gocache", "GOPATH": "/custom/gopath", "GOMODCACHE": "/custom/modcache", "GOLANGCI_LINT_CACHE": "/custom/lintcache"}, false},
		{"InjectsConfiguredTimezone", []string{"EXISTING=1"}, true, []string{"America/Los_Angeles"}, nil, map[string]string{"TZ": "America/Los_Angeles"}, false},
		{"PreservesExplicitTZ", []string{"TZ=UTC"}, true, []string{"America/Los_Angeles"}, nil, map[string]string{"TZ": "UTC"}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runtimeBase := ""
			var tmpDir string
			if tc.useTmpDir {
				tmpDir = t.TempDir()
				runtimeBase = tmpDir
			}
			res := buildPoolEnv(tc.base, runtimeBase, tc.tz...)
			if tc.wantExact != nil {
				if len(res) != len(tc.wantExact) || res[0] != tc.wantExact[0] || res[1] != tc.wantExact[1] {
					t.Errorf("expected exact %v, got %v", tc.wantExact, res)
				}
				return
			}
			envMap := envSliceToMap(res)
			for k, v := range tc.wantEnv {
				expected := strings.ReplaceAll(v, "<tmp>", tmpDir)
				if envMap[k] != expected {
					t.Errorf("expected %s=%q, got %q", k, expected, envMap[k])
				}
			}
			if tc.checkDirs {
				for _, d := range []string{
					filepath.Join(tmpDir, "cache", "go-build"),
					filepath.Join(tmpDir, "cache", "go", "pkg", "mod"),
					filepath.Join(tmpDir, "cache", "golangci-lint"),
				} {
					if fi, err := os.Stat(d); err != nil || !fi.IsDir() {
						t.Errorf("expected directory %q to exist, err: %v", d, err)
					}
				}
			}
		})
	}
}

type mockSequentialPool struct {
	name       string
	initCalled bool
	onInit     func(name string)
}

func (m *mockSequentialPool) AcquireLease(ctx context.Context, targetKey string) (runner.SessionLease, error) {
	return nil, nil
}

func (m *mockSequentialPool) Initialize(ctx context.Context) error {
	m.initCalled = true
	if m.onInit != nil {
		m.onInit(m.name)
	}
	return nil
}

func (m *mockSequentialPool) Close() error {
	return nil
}

func TestPrewarmProcessPoolsSequentially(t *testing.T) {
	initCh := make(chan string, 3)

	p1 := &mockSequentialPool{
		name:   "pool1",
		onInit: func(name string) { initCh <- name },
	}
	p2 := &mockSequentialPool{
		name:   "pool2",
		onInit: func(name string) { initCh <- name },
	}
	p3 := &mockSequentialPool{
		name:   "pool3",
		onInit: func(name string) { initCh <- name },
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	PrewarmProcessPoolsSequentially(ctx, p1, nil, p2, p3)

	var order []string
	for i := 0; i < 3; i++ {
		select {
		case name := <-initCh:
			order = append(order, name)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for pool %d to initialize", i+1)
		}
	}

	if order[0] != "pool1" || order[1] != "pool2" || order[2] != "pool3" {
		t.Errorf("expected sequential order [pool1, pool2, pool3], got %v", order)
	}

	t.Run("CancelledContext", func(t *testing.T) {
		cancelCtx, cancelFn := context.WithCancel(context.Background())
		cancelFn() // cancelled upfront

		done := make(chan struct{})
		pCancelled := &mockSequentialPool{
			name: "pCancelled",
			onInit: func(string) {
				close(done)
			},
		}
		PrewarmProcessPoolsSequentially(cancelCtx, pCancelled)
		select {
		case <-done:
		case <-time.After(50 * time.Millisecond):
		}
	})
}
