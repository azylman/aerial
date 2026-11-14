package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/bwmarrin/discordgo"
)

type voicePoolWiringCase struct {
	name          string
	cfg           *config.Config
	wantGeminiAPI bool
}

func voicePoolWiringCases() []voicePoolWiringCase {
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
			cd.McpServers = config.TargetMcpConfig{
				Voice: map[string]json.RawMessage{
					"ha": json.RawMessage(`{"serverUrl":"http://127.0.0.1:1/mcp"}`),
				},
			}
			cd.Voice.AmbientContext = &config.AmbientContextConfig{
				CacheTTL: "30s",
				Template: tmpl,
				Tools:    []config.AmbientToolConfig{{Name: "ha", Tool: "get_state"}},
			}
		}
		return config.NewFromData(cd)
	}

	return []voicePoolWiringCase{
		{"GeminiAPI_WithHarnessAPIKey", mkCfg("gemini_api", "gemini-2.5-flash", "test-harness-key", "", ""), true},
		{"GeminiAPI_WithAmbientContext", mkCfg("gemini_api", "", "", "test-ambient-key", "Status: {{.ha.state}}"), true},
		{"GeminiAPI_WithAmbientContext_InvalidTemplate", mkCfg("gemini_api", "", "", "test-ambient-key", "{{.invalid"), true},
		{"GeminiAPI_WithAPIKeyFallback", mkCfg("gemini_api", "", "", "test-standard-key", ""), true},
		{"GeminiAPI_EmptyKeyFallbackToAgy", mkCfg("gemini_api", "", "", "", ""), false},
		{"AgyEngine", mkCfg("agy", "", "some-key", "", ""), false},
		{"NilConfig", nil, false},
	}
}

func runVoicePoolWiringCase(t *testing.T, tc voicePoolWiringCase, voiceHome string, spawner runner.DaemonSpawner) {
	t.Helper()
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
}

func runVoicePoolHotReload(t *testing.T, voiceHome string) {
	t.Helper()
	data := &config.ConfigData{
		HarnessAPIKey: "harness-key",
		Voice: config.VoiceConfig{
			Engine: "agy",
			Model:  "agy-model",
		},
	}
	cfg := config.NewFromData(data)

	var mu sync.Mutex
	var lastSpawnedModel, lastSpawnedEngine, lastSpawnedClaudeBin string
	trackingSpawner := &mockMainDaemonSpawner{
		SpawnFn: func(ctx context.Context, dCfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			mu.Lock()
			lastSpawnedModel = dCfg.Model
			lastSpawnedEngine = dCfg.Engine
			lastSpawnedClaudeBin = dCfg.ClaudeBin
			mu.Unlock()
			return newMockMainDaemonSpawner().Spawn(ctx, dCfg)
		},
	}

	pool := createVoiceProcessPool(cfg, voiceHome, "fallback-model", trackingSpawner)
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
	mu.Lock()
	initialModel := lastSpawnedModel
	mu.Unlock()
	if initialModel != "agy-model" {
		t.Fatalf("expected initial spawned model to be agy-model, got %q", initialModel)
	}

	// Update config to agy with new model first to verify agy hot-reload
	cfg.Update(&config.ConfigData{
		HarnessAPIKey:  "harness-key",
		LowEffortModel: "gpt-oss-120b-low",
		Voice: config.VoiceConfig{
			Engine: "agy",
			Model:  "gpt-oss-120b-medium",
		},
	})
	dynPool.MarkDirty()
	if lease, err := dynPool.AcquireLease(context.Background(), "k"); err != nil {
		t.Fatalf("AcquireLease failed after agy model reload: %v", err)
	} else {
		_ = lease.Release()
	}
	mu.Lock()
	reloadedModel := lastSpawnedModel
	mu.Unlock()
	if reloadedModel != "gpt-oss-120b-medium" {
		t.Fatalf("expected reloaded spawned model to be gpt-oss-120b-medium, got %q", reloadedModel)
	}

	// Update config to agy without explicit Voice.Model to verify fallback to LowEffortModel
	cfg.Update(&config.ConfigData{
		HarnessAPIKey:  "harness-key",
		LowEffortModel: "gpt-oss-120b-low",
		Voice: config.VoiceConfig{
			Engine: "agy",
		},
	})
	dynPool.MarkDirty()
	if lease, err := dynPool.AcquireLease(context.Background(), "k"); err != nil {
		t.Fatalf("AcquireLease failed after agy fallback reload: %v", err)
	} else {
		_ = lease.Release()
	}
	mu.Lock()
	fallbackModel := lastSpawnedModel
	mu.Unlock()
	if fallbackModel != "gpt-oss-120b-low" {
		t.Fatalf("expected fallback spawned model to be gpt-oss-120b-low, got %q", fallbackModel)
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

	// Update config to claude engine and verify voice pool hot-reloads to claude
	cfg.Update(&config.ConfigData{
		Engine:    "claude",
		ClaudeBin: "/custom/bin/claude",
		Voice: config.VoiceConfig{
			Engine: "agy",
			Model:  "claude-voice-model",
		},
	})
	dynPool.MarkDirty()
	if lease, err := dynPool.AcquireLease(context.Background(), "k"); err != nil {
		t.Fatalf("AcquireLease failed after claude engine reload: %v", err)
	} else {
		_ = lease.Release()
	}
	mu.Lock()
	reloadedEngine := lastSpawnedEngine
	reloadedClaudeBin := lastSpawnedClaudeBin
	mu.Unlock()
	if reloadedEngine != "claude" || reloadedClaudeBin != "/custom/bin/claude" {
		t.Fatalf("expected voice pool to spawn claude with /custom/bin/claude, got engine=%q bin=%q", reloadedEngine, reloadedClaudeBin)
	}
}

func TestVoicePool_FactoryWiring(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	voiceHome := filepath.Join(tmpDir, "voice")
	spawner := newMockMainDaemonSpawner()

	tests := voicePoolWiringCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runVoicePoolWiringCase(t, tc, voiceHome, spawner)
		})
	}

	t.Run("DynamicHotReload_EngineSwitch", func(t *testing.T) {
		t.Parallel()
		runVoicePoolHotReload(t, voiceHome)
	})
}

func TestExtractMCPServers(t *testing.T) {
	t.Parallel()

	// 1. Nil config
	nilVoice := extractMCPServers(nil, false)
	if len(nilVoice) != 0 {
		t.Fatalf("expected 0 servers for nil voice config, got: %+v", nilVoice)
	}
	nilAll := extractMCPServers(nil, true)
	if len(nilAll) != 0 {
		t.Fatalf("expected 0 servers for nil all config, got: %+v", nilAll)
	}

	// 2. Populated config with Common, Voice, Discord
	rawValid := json.RawMessage(`{"serverUrl":"http://alpha:8080/mcp","headers":{"Auth":"Bearer tok"}}`)
	rawDiscord := json.RawMessage(`{"serverUrl":"http://discord:8080/mcp"}`)
	rawVoice := json.RawMessage(`{"serverUrl":"http://voice:8080/mcp"}`)
	rawSched := json.RawMessage(`{"serverUrl":"http://haro.aerial/mcp"}`)
	data := &config.ConfigData{
		McpServers: config.TargetMcpConfig{
			Common: map[string]json.RawMessage{
				"scheduler": rawSched,
				"alpha":     rawValid,
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

type buildPoolEnvCase struct {
	name      string
	base      []string
	useTmpDir bool
	tz        []string
	wantExact []string
	wantEnv   map[string]string
	checkDirs bool
}

func assertBuildPoolEnvCase(t *testing.T, tc buildPoolEnvCase) {
	t.Helper()
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
}

func TestBuildPoolEnv(t *testing.T) {
	t.Parallel()

	tests := []buildPoolEnvCase{
		{name: "EmptyRuntimeBaseReturnsBaseEnv", base: []string{"FOO=bar", "BAZ=qux"}, wantExact: []string{"FOO=bar", "BAZ=qux"}},
		{name: "InjectsUnifiedCacheDirectories", base: []string{"EXISTING=1"}, useTmpDir: true, wantEnv: map[string]string{"GOCACHE": "<tmp>/cache/go-build", "GOPATH": "<tmp>/cache/go", "GOMODCACHE": "<tmp>/cache/go/pkg/mod", "GOLANGCI_LINT_CACHE": "<tmp>/cache/golangci-lint", "EXISTING": "1"}, checkDirs: true},
		{name: "PreservesExplicitCacheEnv", base: []string{"GOCACHE=/custom/gocache", "GOPATH=/custom/gopath", "GOMODCACHE=/custom/modcache", "GOLANGCI_LINT_CACHE=/custom/lintcache"}, useTmpDir: true, wantEnv: map[string]string{"GOCACHE": "/custom/gocache", "GOPATH": "/custom/gopath", "GOMODCACHE": "/custom/modcache", "GOLANGCI_LINT_CACHE": "/custom/lintcache"}},
		{name: "InjectsConfiguredTimezone", base: []string{"EXISTING=1"}, useTmpDir: true, tz: []string{"America/Los_Angeles"}, wantEnv: map[string]string{"TZ": "America/Los_Angeles"}},
		{name: "PreservesExplicitTZ", base: []string{"TZ=UTC"}, useTmpDir: true, tz: []string{"America/Los_Angeles"}, wantEnv: map[string]string{"TZ": "UTC"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertBuildPoolEnvCase(t, tc)
		})
	}
}

func TestBuildPoolEnv_RootChown(t *testing.T) {
	origGetEuid := getEuid
	origChown := chownFile
	defer func() {
		getEuid = origGetEuid
		chownFile = origChown
	}()

	var chowned []string
	getEuid = func() int { return 0 }
	chownFile = func(path string, uid, gid int) error {
		chowned = append(chowned, path)
		return nil
	}

	tmpDir := t.TempDir()
	_ = buildPoolEnv([]string{"A=B"}, tmpDir)
	if len(chowned) == 0 {
		t.Errorf("expected chownFile to be called for cache directories when getEuid == 0")
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

	ctx := t.Context()

	PrewarmProcessPoolsSequentially(ctx, p1, nil, p2, p3)

	var order []string
	for i := range 3 {
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
}

type mockErrClosingPool struct {
	runner.AgentPool

	closed bool
}

func (m *mockErrClosingPool) Close() error {
	m.closed = true
	return errors.New("mock pool close error")
}

func TestCloseAllBrainPools_Error(t *testing.T) {
	pool := &mockErrClosingPool{}
	closeAllBrainPools([]runner.AgentPool{pool})
	if !pool.closed {
		t.Fatal("expected pool Close to be called")
	}
}

func TestNewAmbientContextResolver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        *config.Config
		ambCfg     *config.AmbientContextConfig
		repeatCall bool
		wantErr    bool
		wantSubstr string
	}{
		{
			name:   "NilAmbConfig",
			cfg:    nil,
			ambCfg: nil,
		},
		{
			name: "RenderValidTemplateAndCacheHit",
			cfg:  newTestConfig(func(d *config.ConfigData) {}),
			ambCfg: &config.AmbientContextConfig{
				Template: "Static ambient text",
				CacheTTL: "1m",
			},
			repeatCall: true,
			wantSubstr: "Static ambient text",
		},
		{
			name: "InvalidTemplateError",
			cfg:  newTestConfig(func(d *config.ConfigData) {}),
			ambCfg: &config.AmbientContextConfig{
				Template: "{{.unclosed",
				CacheTTL: "1m",
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resolver := newAmbientContextResolver(tc.cfg)
			res, err := resolver(context.Background(), tc.ambCfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantSubstr != "" && !strings.Contains(res, tc.wantSubstr) {
				t.Errorf("expected result to contain %q, got %q", tc.wantSubstr, res)
			}
			if tc.repeatCall {
				res2, err2 := resolver(context.Background(), tc.ambCfg)
				if err2 != nil {
					t.Fatalf("unexpected error on repeated call: %v", err2)
				}
				if res2 != res {
					t.Errorf("expected cached result %q, got %q", res, res2)
				}
			}
		})
	}
}

type serveBrainHTTPCase struct {
	name       string
	closeLn    bool
	asyncErr   error
	cancelCtx  bool
	wantErr    bool
	wantErrSub string
}

func assertServeBrainHTTPCase(t *testing.T, tc serveBrainHTTPCase) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind listener: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var asyncErrCh chan error
	if tc.asyncErr != nil {
		asyncErrCh = make(chan error, 1)
		asyncErrCh <- tc.asyncErr
	}

	if tc.closeLn {
		_ = ln.Close()
	}
	if tc.cancelCtx {
		cancel()
	}

	mux := http.NewServeMux()
	var errResult error
	if asyncErrCh != nil {
		errResult = serveBrainHTTP(ctx, ln, mux, "test-model", asyncErrCh)
	} else {
		errResult = serveBrainHTTP(ctx, ln, mux, "test-model")
	}

	if tc.wantErr {
		if errResult == nil {
			t.Fatalf("expected error, got nil")
		}
		if tc.wantErrSub != "" && !strings.Contains(errResult.Error(), tc.wantErrSub) {
			t.Errorf("expected error containing %q, got %v", tc.wantErrSub, errResult)
		}
		return
	}
	if errResult != nil {
		t.Fatalf("unexpected error: %v", errResult)
	}
}

func TestServeBrainHTTP(t *testing.T) {
	t.Parallel()

	tests := []serveBrainHTTPCase{
		{name: "GracefulShutdownOnContextDone", cancelCtx: true},
		{name: "AsyncLeaderError", asyncErr: errors.New("leader failed"), wantErr: true, wantErrSub: "leader failed"},
		{name: "ListenerClosedServeError", closeLn: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertServeBrainHTTPCase(t, tc)
		})
	}
}

type leaderHandoffCase struct {
	name             string
	lockErr          error
	cancelWait       bool
	cancelViaRelease bool
	holdThenUnblk    bool
	wantErr          bool
	wantReleased     bool
}

func assertLeaderHandoffCase(t *testing.T, tc leaderHandoffCase) {
	t.Helper()
	store := dbtest.NewFakeStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lockEntered := make(chan struct{})
	unblockLock := make(chan struct{})
	var released atomic.Bool

	store.SetLeaderLockHook(func(lockCtx context.Context) (func(), error) {
		close(lockEntered)
		if tc.lockErr != nil {
			return nil, tc.lockErr
		}
		if tc.cancelWait {
			<-lockCtx.Done()
			return nil, lockCtx.Err()
		}
		if tc.holdThenUnblk {
			<-unblockLock
		}
		return func() { released.Store(true) }, nil
	})

	tmpDir := t.TempDir()
	cfg := newTestConfig(func(d *config.ConfigData) {
		d.AgyBin = "/bin/true"
		d.Model = "gemini-2.5-flash"
		d.GeminiHomeDir = tmpDir
		d.DataDir = tmpDir
	})
	ephemPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-2.5-flash"}, newMockMainDaemonSpawner())
	defer func() { _ = ephemPool.Close() }()

	doneCh := make(chan struct{}, 1)
	pool := queue.New(cfg, queue.WorkerPoolConfig{
		Store: ephemStore(store),
		RunnerFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
			return `{"conversation_id":"11111111-2222-3333-4444-555555555555","status":"SUCCESS","response":"ok"}`, "", 0, nil
		},
		DeliveryFunc: func(s *discordgo.Session, c, txt string) error { return nil },
		OnMessageCompleted: func(m db.Message, status string) {
			select {
			case doneCh <- struct{}{}:
			default:
			}
		},
	})
	pool.HoldUntilLeader()
	pool.Start()
	defer pool.Stop()

	errCh, stopHandoff, releaseLock := startLeaderHandoff(ctx, store, store, pool, nil, ephemPool, cfg, cfg.Current())
	<-lockEntered

	if tc.holdThenUnblk {
		msg := db.Message{ID: "m-canary-1", ThreadID: "1542423172400291873", Content: "held prompt", Status: db.StatusPending, CreatedAt: time.Now().UTC()}
		_ = store.InsertMessage(ctx, msg)
		pool.Enqueue(msg)
		close(unblockLock)
		select {
		case <-doneCh:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for held canary message after leader promotion")
		}
	}
	if tc.cancelWait && !tc.cancelViaRelease {
		cancel()
	}
	if tc.wantErr {
		select {
		case err := <-errCh:
			if err == nil {
				t.Fatal("expected non-nil leader lock error")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for leader lock error")
		}
	}

	stopHandoff()
	if released.Load() {
		t.Fatalf("expected lock not to be released immediately after stopHandoff")
	}
	releaseLock()
	if released.Load() != tc.wantReleased {
		t.Errorf("released = %v, want %v", released.Load(), tc.wantReleased)
	}
}

func TestStartLeaderHandoff_Scenarios(t *testing.T) {
	t.Parallel()

	tests := []leaderHandoffCase{
		{name: "CanaryHoldAndPromotion", holdThenUnblk: true, wantReleased: true},
		{name: "LeaderLockErrorPropagates", lockErr: errors.New("advisory lock failed"), wantErr: true},
		{name: "CancelledContextWhileWaiting", cancelWait: true},
		{name: "ReleaseWhileWaitingWithoutParentCancel", cancelWait: true, cancelViaRelease: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertLeaderHandoffCase(t, tc)
		})
	}
}

func ephemStore(s db.Store) db.Store { return s }
