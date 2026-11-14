package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/session"
	"github.com/google/uuid"
)

func (p *UnifiedProcessPool) HasDaemon(targetKey string) bool {
	_, ok := p.Get(targetKey)
	return ok
}

func (p *UnifiedProcessPool) WaitBackground() {
	if p != nil {
		p.bgWg.Wait()
	}
}

type pipeMockSpawnerOpts struct {
	pid               int
	killErr           error
	spawnErrOn        int
	failSpawn         *atomic.Bool
	skipInitOnAttempt int
	closeIn           bool
	initSessID        string
	onSpawn           func(cfg DaemonConfig)
	echoRes           string
	onUser            func(attempt int, line string) (resp, errStr, status string)
}

func handlePipeMockUserLine(outW io.Writer, line string, attemptCount *atomic.Int32, opts pipeMockSpawnerOpts) {
	if opts.onUser != nil {
		att := int(attemptCount.Add(1))
		resp, errStr, status := opts.onUser(att, line)
		if status == "" {
			if errStr != "" && resp == "" {
				status = "ERROR"
			} else {
				status = "SUCCESS"
			}
		}
		_, _ = fmt.Fprintf(outW, `{"event":"result","response":%q,"error":%q,"status":%q}`+"\n", resp, errStr, status)
		return
	}
	if opts.echoRes != "" {
		_, _ = fmt.Fprintf(outW, `{"event":"result","result":{"status":"SUCCESS","response":%q}}`+"\n", opts.echoRes)
	}
}

func runPipeMockPump(inR io.Reader, outW io.WriteCloser, sessID string, idx int, attemptCount *atomic.Int32, opts pipeMockSpawnerOpts) {
	defer outW.Close()
	if opts.skipInitOnAttempt == 0 || idx != opts.skipInitOnAttempt {
		_, _ = fmt.Fprintf(outW, `{"event":"init","session_id":%q}`+"\n", sessID)
	}
	buf := make([]byte, 4096)
	for {
		n, err := inR.Read(buf)
		if err != nil {
			return
		}
		line := string(buf[:n])
		if strings.Contains(line, `"event":"user"`) {
			handlePipeMockUserLine(outW, line, attemptCount, opts)
		}
	}
}

func resolvePipeMockSessionID(cfg DaemonConfig, idx int, initSessID string) string {
	if cfg.SessionID != "" {
		return cfg.SessionID
	}
	if idx > 1 || initSessID == "" {
		return fmt.Sprintf("00000000-0000-0000-0000-%012d", idx)
	}
	return initSessID
}

func newPipeMockSpawner(opts pipeMockSpawnerOpts) *MockDaemonSpawner {
	var count, attemptCount atomic.Int32
	return &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			idx := int(count.Add(1))
			if opts.onSpawn != nil {
				opts.onSpawn(cfg)
			}
			if (opts.failSpawn != nil && opts.failSpawn.Load()) || (opts.spawnErrOn > 0 && idx == opts.spawnErrOn) {
				return nil, nil, nil, nil, errors.New("daemon spawn failed")
			}
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			if opts.closeIn {
				_ = inR.Close()
				_ = inW.Close()
			}

			sessID := resolvePipeMockSessionID(cfg, idx, opts.initSessID)
			go runPipeMockPump(inR, outW, sessID, idx, &attemptCount, opts)

			pid := opts.pid
			if pid == 0 {
				pid = idx * 100
			}
			return inW, outR, errR, &MockProcessHandle{pid: pid, killErr: opts.killErr}, nil
		},
	}
}

func TestUnifiedProcessPool_SingleflightPrewarming(t *testing.T) {
	var spawnCount atomic.Int32
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		initSessID: "00000000-0000-0000-0000-000000000010",
		onSpawn:    func(cfg DaemonConfig) { spawnCount.Add(1) },
	})
	pool := NewUnifiedProcessPool(PoolConfig{PrewarmedTargets: []string{"kiosk"}, DefaultModel: "gemini-2.5-pro", MaxIdle: 24 * time.Hour}, mock)
	defer pool.Close()
	if err := pool.Initialize(context.Background()); err != nil {
		t.Fatalf("failed to initialize pool: %v", err)
	}
	d1, err1 := pool.GetOrCreate(context.Background(), "kiosk", "")
	d2, err2 := pool.GetOrCreate(context.Background(), "kiosk", "")
	if err1 != nil || err2 != nil || d1 != d2 || spawnCount.Load() != 1 {
		t.Fatalf("kiosk get failed: d1=%p d2=%p err1=%v err2=%v count=%d", d1, d2, err1, err2, spawnCount.Load())
	}
}

func TestUnifiedProcessPool_Defaults(t *testing.T) {
	pool := NewUnifiedProcessPool(PoolConfig{}, nil)
	if pool.spawner == nil || pool.cfg.MaxIdle != 24*time.Hour {
		t.Errorf("unexpected defaults: spawner=%v MaxIdle=%v", pool.spawner, pool.cfg.MaxIdle)
	}
}

type modelResolutionCase struct {
	name, model, defaultModel, updateModel, wantModel, wantUpdatedModel                        string
	engine, updateEngine, updateClaudeBin, wantEngine, wantUpdatedEngine, wantUpdatedClaudeBin string
	nilPool, checkDaemon                                                                       bool
}

func assertDaemonSpawnCfg(t *testing.T, pool *UnifiedProcessPool, mock *MockSpawner, wantModel, wantEngine, wantBin string, step string) {
	t.Helper()
	daemon, err := pool.GetOrCreate(context.Background(), "target-1", "")
	if err != nil || daemon == nil {
		t.Fatalf("[%s] unexpected GetOrCreate result: daemon=%v err=%v", step, daemon, err)
	}
	mock.mu.Lock()
	spawnCfg := mock.lastSpawnCfg
	mock.mu.Unlock()
	if spawnCfg.Model != wantModel {
		t.Fatalf("[%s] expected model %s, got %s", step, wantModel, spawnCfg.Model)
	}
	if wantEngine != "" && spawnCfg.Engine != wantEngine {
		t.Fatalf("[%s] expected engine %s, got %s", step, wantEngine, spawnCfg.Engine)
	}
	if wantBin != "" && spawnCfg.ClaudeBin != wantBin {
		t.Fatalf("[%s] expected bin %s, got %s", step, wantBin, spawnCfg.ClaudeBin)
	}
}

func runModelResolutionCase(t *testing.T, tc modelResolutionCase) {
	t.Helper()
	if tc.nilPool {
		var p *UnifiedProcessPool
		p.UpdateModel(tc.updateModel)
		p.UpdateEngine(tc.updateEngine, tc.updateClaudeBin)
		return
	}
	mock := NewMockSpawner()
	pool := NewUnifiedProcessPool(PoolConfig{Model: tc.model, DefaultModel: tc.defaultModel, Engine: tc.engine}, mock)
	defer pool.Close()
	if got := pool.cfg.Model; got != tc.wantModel {
		t.Fatalf("expected pool model %q, got %q", tc.wantModel, got)
	}
	if tc.wantEngine != "" && pool.cfg.Engine != tc.wantEngine {
		t.Fatalf("expected pool engine %q, got %q", tc.wantEngine, pool.cfg.Engine)
	}
	if tc.checkDaemon {
		assertDaemonSpawnCfg(t, pool, mock, tc.wantModel, tc.wantEngine, "", "initial")
	}
	if tc.updateModel != "" {
		pool.UpdateModel(tc.updateModel)
	}
	if tc.updateEngine != "" {
		pool.UpdateEngine(tc.updateEngine, tc.updateClaudeBin)
	}
	pool.MarkDirty()
	pool.WaitBackground()
	if got := pool.cfg.Model; got != tc.wantUpdatedModel {
		t.Fatalf("expected updated pool model %q, got %q", tc.wantUpdatedModel, got)
	}
	if tc.wantUpdatedEngine != "" && pool.cfg.Engine != tc.wantUpdatedEngine {
		t.Fatalf("expected updated pool engine %q, got %q", tc.wantUpdatedEngine, pool.cfg.Engine)
	}
	if tc.wantUpdatedClaudeBin != "" && pool.cfg.ClaudeBin != tc.wantUpdatedClaudeBin {
		t.Fatalf("expected updated claude bin %q, got %q", tc.wantUpdatedClaudeBin, pool.cfg.ClaudeBin)
	}
	if tc.checkDaemon {
		assertDaemonSpawnCfg(t, pool, mock, tc.wantUpdatedModel, tc.wantUpdatedEngine, tc.wantUpdatedClaudeBin, "updated")
	}
}

func TestUnifiedProcessPool_ModelResolution(t *testing.T) {
	t.Parallel()
	tests := []modelResolutionCase{
		{name: "NilPoolUpdateModel", nilPool: true, updateModel: "claude-sonnet-5-5-low"},
		{name: "ModelBoundAndHotReload", model: "gemini-3.8-flash-low", updateModel: "gpt-oss-120b-medium", wantModel: "gemini-3.8-flash-low", wantUpdatedModel: "gpt-oss-120b-medium", checkDaemon: true},
		{name: "ModelFallbackAndEmptyUpdateIgnored", defaultModel: "gemini-2.5-pro", updateModel: "   ", wantModel: "gemini-2.5-pro", wantUpdatedModel: "gemini-2.5-pro"},
		{name: "UpdateEngineSwitchToClaude", model: "gemini-3.8-flash-low", wantModel: "gemini-3.8-flash-low", wantUpdatedModel: "gemini-3.8-flash-low", engine: "agy", updateEngine: "claude", updateClaudeBin: "/opt/bin/claude", wantEngine: "agy", wantUpdatedEngine: "claude", wantUpdatedClaudeBin: "/opt/bin/claude", checkDaemon: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runModelResolutionCase(t, tc)
		})
	}
}

func TestUnifiedProcessPool_GetOrCreateAndReuse(t *testing.T) {
	t.Parallel()
	var nilPool *UnifiedProcessPool
	if err := nilPool.Close(); err != nil {
		t.Errorf("expected nil error from nilPool.Close()")
	}

	var reuseSpawnCount atomic.Int32
	reuseMock := newPipeMockSpawner(pipeMockSpawnerOpts{
		initSessID: "00000000-0000-0000-0000-000000000001",
		onSpawn:    func(cfg DaemonConfig) { reuseSpawnCount.Add(1) },
	})
	reusePool := NewUnifiedProcessPool(PoolConfig{}, reuseMock)
	defer reusePool.Close()

	ctx := context.Background()
	d1, err := reusePool.GetOrCreate(ctx, "thread-1", "")
	if err != nil {
		t.Fatalf("first GetOrCreate failed: %v", err)
	}
	d2, err := reusePool.GetOrCreate(ctx, "thread-1", "")
	if err != nil || d1 != d2 || reuseSpawnCount.Load() != 1 {
		t.Fatalf("reuse failed: d1=%p d2=%p count=%d", d1, d2, reuseSpawnCount.Load())
	}
	if err := d1.Close(); err != nil {
		t.Fatalf("failed closing d1: %v", err)
	}
	d3, err := reusePool.GetOrCreate(ctx, "thread-1", "")
	if err != nil || d3 == d1 || reuseSpawnCount.Load() != 2 {
		t.Fatalf("replacement failed: d1=%p d3=%p count=%d", d1, d3, reuseSpawnCount.Load())
	}

	var multiSpawnCount atomic.Int32
	multiMock := newPipeMockSpawner(pipeMockSpawnerOpts{
		onSpawn: func(cfg DaemonConfig) { multiSpawnCount.Add(1) },
	})
	multiPool := NewUnifiedProcessPool(PoolConfig{}, multiMock)
	defer multiPool.Close()

	dAlpha, errAlpha := multiPool.GetOrCreate(ctx, "thread-alpha", "")
	dVoice, errVoice := multiPool.GetOrCreate(ctx, "device-voice", "")
	if errAlpha != nil || errVoice != nil || dAlpha == dVoice || multiSpawnCount.Load() != 2 {
		t.Errorf("multiple targets failed: count=%d", multiSpawnCount.Load())
	}
}

func TestUnifiedProcessPool_SpawnCloseAndPrewarmErrors(t *testing.T) {
	t.Parallel()
	spawnFailMock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return nil, nil, nil, nil, errors.New("simulated spawn failure")
		},
	}
	spawnFailPool := NewUnifiedProcessPool(PoolConfig{}, spawnFailMock)
	defer spawnFailPool.Close()
	if _, err := spawnFailPool.GetOrCreate(context.Background(), "failed-target", ""); err == nil || !strings.Contains(err.Error(), "failed to start daemon") {
		t.Errorf("expected failed to start daemon error, got %v", err)
	}

	closedPool := NewUnifiedProcessPool(PoolConfig{}, newPipeMockSpawner(pipeMockSpawnerOpts{}))
	if err := closedPool.Close(); err != nil || closedPool.Initialize(context.Background()) == nil {
		t.Fatalf("expected close success and Initialize error on closed pool")
	}
	if _, err := closedPool.GetOrCreate(context.Background(), "any", ""); err == nil {
		t.Error("expected error calling GetOrCreate on closed pool")
	}

	closeErrPool := NewUnifiedProcessPool(PoolConfig{}, newPipeMockSpawner(pipeMockSpawnerOpts{pid: 600, killErr: errors.New("kill failed")}))
	if _, err := closeErrPool.GetOrCreate(context.Background(), "error-target", ""); err != nil {
		t.Fatalf("failed GetOrCreate: %v", err)
	}
	err1 := closeErrPool.Close()
	if err1 == nil || !strings.Contains(err1.Error(), "errors closing pool daemons") || closeErrPool.Close().Error() != err1.Error() {
		t.Fatalf("expected idempotent close error, got %v", err1)
	}

	prewarmFailMock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return nil, nil, nil, nil, errors.New("prewarm boom")
		},
	}
	prewarmPool := NewUnifiedProcessPool(PoolConfig{PrewarmedTargets: []string{"bad-prewarm"}}, prewarmFailMock)
	defer prewarmPool.Close()
	if err := prewarmPool.Initialize(context.Background()); err != nil {
		t.Fatalf("unexpected Initialize error: %v", err)
	}
}

func TestUnifiedProcessPool_CloseDuringSpawn(t *testing.T) {
	spawnStarted, allowSpawn := make(chan struct{}), make(chan struct{})
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		pid:     500,
		killErr: errors.New("fail kill"),
		onSpawn: func(cfg DaemonConfig) {
			close(spawnStarted)
			<-allowSpawn
		},
	})
	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	errChan := make(chan error, 1)
	go func() {
		_, err := pool.GetOrCreate(context.Background(), "slow-target", "")
		errChan <- err
	}()
	<-spawnStarted
	_ = pool.Close()
	close(allowSpawn)
	if err := <-errChan; err == nil || !strings.Contains(err.Error(), "process pool is closed") {
		t.Fatalf("expected process pool closed error, got %v", err)
	}
}

func TestUnifiedProcessPool_Initialize_Sequential(t *testing.T) {
	var mu sync.Mutex
	var activeSpawns, maxConcurrentSpawns int
	var order []string
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		onSpawn: func(cfg DaemonConfig) {
			mu.Lock()
			activeSpawns++
			if activeSpawns > maxConcurrentSpawns {
				maxConcurrentSpawns = activeSpawns
			}
			order = append(order, cfg.ThreadID)
			activeSpawns--
			mu.Unlock()
		},
	})
	pool := NewUnifiedProcessPool(PoolConfig{PrewarmedTargets: []string{"seq-target-1", "seq-target-2", "seq-target-3"}}, mock)
	defer pool.Close()
	if err := pool.Initialize(context.Background()); err != nil {
		t.Fatalf("unexpected Initialize error: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if maxConcurrentSpawns != 1 || len(order) != 3 || order[0] != "seq-target-1" || order[1] != "seq-target-2" || order[2] != "seq-target-3" {
		t.Errorf("sequential initialization check failed: max=%d, order=%v", maxConcurrentSpawns, order)
	}
}

func TestUnifiedProcessPool_Initialize_ContextCancellation(t *testing.T) {
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{pid: 100})
	pool := NewUnifiedProcessPool(PoolConfig{PrewarmedTargets: []string{"cancel-target-1", "cancel-target-2"}}, mock)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pool.Initialize(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func createTranscriptFixture(tmpDir, sessID string, steps, turns int, bBytes, dbBytes int64) {
	logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessID, ".system_generated", "logs")
	_ = os.MkdirAll(logsDir, 0o755)
	tPath := filepath.Join(logsDir, "transcript.jsonl")
	if steps > 0 {
		_ = os.WriteFile(tPath, []byte(fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\n", steps)), 0o644)
	}
	if bBytes > 0 {
		_ = os.WriteFile(tPath, make([]byte, bBytes), 0o644)
	}
	if dbBytes > 0 {
		convDir := filepath.Join(tmpDir, "conversations")
		_ = os.MkdirAll(convDir, 0o755)
		_ = os.WriteFile(filepath.Join(convDir, sessID+".db"), make([]byte, dbBytes), 0o644)
	}
	if turns > 0 {
		var sb strings.Builder
		for i := range turns {
			fmt.Fprintf(&sb, `{"step_index": %d, "source": "USER_EXPLICIT", "type": "USER_INPUT", "content": "Turn %d"}`+"\n", i*2, i+1)
			fmt.Fprintf(&sb, `{"step_index": %d, "source": "MODEL", "type": "PLANNER_RESPONSE", "content": "Resp %d"}`+"\n", i*2+1, i+1)
		}
		_ = os.WriteFile(tPath, []byte(sb.String()), 0o644)
	}
}

func TestUnifiedProcessPool_ShouldRotate(t *testing.T) {
	tests := []struct {
		name             string
		daemon           *StreamingDaemon
		transcriptSteps  int
		transcriptBytes  int64
		sessionDBBytes   int64
		transcriptTurns  int
		poolCfg          PoolConfig
		setupSessionMgr  bool
		wantRotate       bool
		wantReasonSubstr string
	}{
		{name: "nil daemon", wantRotate: false},
		{name: "turn count exceeded", daemon: &StreamingDaemon{turnCount: DefaultMaxSessionTurns, state: StateReady}, wantRotate: true, wantReasonSubstr: "turn count threshold exceeded"},
		{name: "raw daemon step count alone does not trigger", daemon: &StreamingDaemon{turnCount: 2, stepCount: 185, state: StateReady}, wantRotate: false},
		{name: "below threshold", daemon: &StreamingDaemon{turnCount: 5, stepCount: 100, state: StateReady}, wantRotate: false},
		{name: "inflight turn gating", daemon: &StreamingDaemon{turnCount: 15, stepCount: 250, inflight: []*TurnContext{{TurnID: "turn-inflight"}}, state: StateReady}, wantRotate: false},
		{name: "dirty daemon with 0 inflight", daemon: &StreamingDaemon{dirty: true, state: StateReady}, wantRotate: true, wantReasonSubstr: "daemon marked dirty during in-flight turn"},
		{name: "dirty daemon with inflight > 0", daemon: &StreamingDaemon{dirty: true, state: StateExecuting, inflight: []*TurnContext{{TurnID: "turn-inflight"}}}, wantRotate: false},
		{name: "transcript step count threshold exceeded", daemon: &StreamingDaemon{turnCount: DefaultMinSessionTurns, state: StateReady}, setupSessionMgr: true, transcriptSteps: DefaultMaxSessionSteps - 1, wantRotate: true, wantReasonSubstr: "transcript step count threshold exceeded"},
		{name: "transcript file size threshold exceeded", daemon: &StreamingDaemon{turnCount: DefaultMinSessionTurns, state: StateReady}, setupSessionMgr: true, transcriptBytes: session.DefaultMaxTranscriptBytes + 10, wantRotate: true, wantReasonSubstr: "transcript file size threshold exceeded"},
		{name: "session DB size threshold exceeded", daemon: &StreamingDaemon{turnCount: DefaultMinSessionTurns, state: StateReady}, setupSessionMgr: true, sessionDBBytes: session.DefaultMaxSessionDBBytes + 10, wantRotate: true, wantReasonSubstr: "session DB size threshold exceeded"},
		{name: "transcript turn count threshold exceeded", daemon: &StreamingDaemon{state: StateReady}, setupSessionMgr: true, transcriptTurns: DefaultMaxSessionTurns, wantRotate: true, wantReasonSubstr: "transcript turn count threshold exceeded"},
		{name: "under min turns standard thresholds do not rotate", daemon: &StreamingDaemon{turnCount: 1, state: StateReady}, setupSessionMgr: true, transcriptSteps: DefaultMaxSessionSteps, sessionDBBytes: DefaultMaxSessionDBBytes, wantRotate: false},
		{name: "under min turns 3x emergency step blowout rotates", daemon: &StreamingDaemon{turnCount: 1, state: StateReady}, setupSessionMgr: true, transcriptSteps: DefaultEmergencyRotationMultiplier * DefaultMaxSessionSteps, wantRotate: true, wantReasonSubstr: "transcript step count emergency threshold exceeded"},
		{name: "under min turns 3x emergency transcript size blowout rotates", daemon: &StreamingDaemon{turnCount: 1, state: StateReady}, setupSessionMgr: true, transcriptBytes: DefaultEmergencyRotationMultiplier*DefaultMaxTranscriptBytes + 10, wantRotate: true, wantReasonSubstr: "transcript file size emergency threshold exceeded"},
		{name: "under min turns 3x emergency DB size blowout rotates", daemon: &StreamingDaemon{turnCount: 2, state: StateReady}, setupSessionMgr: true, sessionDBBytes: DefaultEmergencyRotationMultiplier*DefaultMaxSessionDBBytes + 10, wantRotate: true, wantReasonSubstr: "session DB size emergency threshold exceeded"},
		{name: "under min turns dirty daemon rotates immediately", daemon: &StreamingDaemon{turnCount: 1, dirty: true, state: StateReady}, wantRotate: true, wantReasonSubstr: "daemon marked dirty"},
		{name: "custom min turns and emergency multiplier", daemon: &StreamingDaemon{turnCount: 3, state: StateReady}, setupSessionMgr: true, sessionDBBytes: 2*DefaultMaxSessionDBBytes + 10, poolCfg: PoolConfig{MinSessionTurns: 5, EmergencyRotationMultiplier: 2}, wantRotate: true, wantReasonSubstr: "session DB size emergency threshold exceeded"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.poolCfg
			tmpDir := t.TempDir()
			if tt.setupSessionMgr {
				cfg.SessionManager = session.New(tmpDir, tmpDir)
				if tt.daemon != nil {
					sessID := "sess-" + uuid.NewString()
					tt.daemon.sessionID = sessID
					createTranscriptFixture(tmpDir, sessID, tt.transcriptSteps, tt.transcriptTurns, tt.transcriptBytes, tt.sessionDBBytes)
				}
			}

			pool := NewUnifiedProcessPool(cfg, nil)
			defer pool.Close()

			gotRotate, gotReason := pool.ShouldRotate(tt.daemon)
			if gotRotate != tt.wantRotate {
				t.Fatalf("ShouldRotate() got %v, want %v (reason: %q)", gotRotate, tt.wantRotate, gotReason)
			}
			if tt.wantReasonSubstr != "" && !strings.Contains(gotReason, tt.wantReasonSubstr) {
				t.Errorf("ShouldRotate() reason %q does not contain %q", gotReason, tt.wantReasonSubstr)
			}
		})
	}
}

func TestUnifiedProcessPool_SetSessionManager(t *testing.T) {
	pool := NewUnifiedProcessPool(PoolConfig{}, nil)
	defer pool.Close()
	sm := session.New(t.TempDir(), t.TempDir())
	pool.SetSessionManager(sm)
	if pool.cfg.SessionManager != sm {
		t.Fatalf("expected session manager to be updated")
	}
	var nilPool *UnifiedProcessPool
	nilPool.SetSessionManager(sm)
}

func TestUnifiedProcessPool_GetOrCreate_PreflightRotationAndGuardrails(t *testing.T) {
	var lastSpawnedSessionID atomic.Pointer[string]
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		onSpawn: func(cfg DaemonConfig) {
			sID := cfg.SessionID
			lastSpawnedSessionID.Store(&sID)
		},
	})

	tmpDir := t.TempDir()
	sessMgr := session.New(tmpDir, tmpDir)
	pool := NewUnifiedProcessPool(PoolConfig{SessionManager: sessMgr}, mock)
	defer pool.Close()

	ctx := context.Background()
	d1, err := pool.GetOrCreate(ctx, "target-1", "")
	if err != nil {
		t.Fatalf("failed creating initial daemon: %v", err)
	}
	d1.mu.Lock()
	d1.turnCount = DefaultMaxSessionTurns
	d1.mu.Unlock()

	d2, err := pool.GetOrCreate(ctx, "target-1", "")
	if err != nil || d2 == d1 {
		t.Fatalf("expected new daemon after pre-flight rotation, got d2=%v err=%v", d2, err)
	}
	pool.WaitBackground()
	if d1.State() != StateClosed {
		t.Fatalf("expected old daemon to be closed, got %v", d1.State())
	}

	guardTests := []struct {
		name            string
		target          string
		sessID          string
		transcriptBytes int64
		sessionDBBytes  int64
		transcriptSteps int
		transcriptTurns int
	}{
		{name: "transcript size exceeded", target: "target-oversized", sessID: "550e8400-e29b-41d4-a716-446655440001", transcriptBytes: session.DefaultMaxTranscriptBytes + 10},
		{name: "db size exceeded", target: "target-oversized-db", sessID: "550e8400-e29b-41d4-a716-446655440002", sessionDBBytes: session.DefaultMaxSessionDBBytes + 10},
		{name: "transcript steps exceeded", target: "target-oversized-steps", sessID: "550e8400-e29b-41d4-a716-446655440003", transcriptSteps: DefaultMaxSessionSteps},
		{name: "transcript turns exceeded", target: "target-oversized-turns", sessID: "550e8400-e29b-41d4-a716-446655440004", transcriptTurns: DefaultMaxSessionTurns},
	}

	for _, gt := range guardTests {
		t.Run(gt.name, func(t *testing.T) {
			createTranscriptFixture(tmpDir, gt.sessID, gt.transcriptSteps, gt.transcriptTurns, gt.transcriptBytes, gt.sessionDBBytes)
			d, err := pool.GetOrCreate(ctx, gt.target, gt.sessID)
			if err != nil || d.SessionID() != gt.sessID {
				t.Fatalf("failed GetOrCreate for oversized session %q: sess=%q err=%v", gt.name, d.SessionID(), err)
			}
		})
	}
}

func TestUnifiedProcessPool_RotateDaemon(t *testing.T) {
	pool := NewUnifiedProcessPool(PoolConfig{}, newPipeMockSpawner(pipeMockSpawnerOpts{}))
	defer pool.Close()
	ctx := context.Background()
	d1, err := pool.GetOrCreate(ctx, "target-session", "")
	if err != nil {
		t.Fatalf("failed creating daemon: %v", err)
	}
	d1.mu.Lock()
	d1.turnCount, d1.stepCount = DefaultMaxSessionTurns, 185
	d1.mu.Unlock()

	d2, err := pool.RotateDaemon(ctx, "target-session", "compacted conversation history summary")
	if err != nil || d2 == d1 || d1.State() != StateClosed || d2.TurnCount() != 0 || d2.StepCount() != 0 {
		t.Fatalf("rotate failed: d1=%v d2=%v err=%v", d1.State(), d2, err)
	}
	d3, err := pool.RotateDaemon(ctx, "non-existent-target", "")
	if err != nil || d3 == nil {
		t.Fatalf("failed rotating non-existent target: d3=%v err=%v", d3, err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("failed closing pool: %v", err)
	}
	if _, errClosed := pool.RotateDaemon(ctx, "target-session", ""); errClosed == nil || !strings.Contains(errClosed.Error(), "process pool is closed") {
		t.Errorf("expected error on closed pool rotation, got %v", errClosed)
	}
}

func TestUnifiedProcessPool_RotateDaemon_Errors(t *testing.T) {
	var failSpawn atomic.Bool
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{pid: 100, killErr: errors.New("kill failed"), failSpawn: &failSpawn})
	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()
	ctx := context.Background()
	d1, err := pool.GetOrCreate(ctx, "kill-err-target", "")
	if err != nil {
		t.Fatalf("failed creating initial daemon: %v", err)
	}
	d2, err := pool.RotateDaemon(ctx, "kill-err-target", "")
	if err != nil || d2 == d1 {
		t.Fatalf("expected RotateDaemon to succeed: err=%v", err)
	}
	failSpawn.Store(true)
	if _, errFail := pool.RotateDaemon(ctx, "fail-spawn-target", ""); errFail == nil || !strings.Contains(errFail.Error(), "failed creating rotated daemon") {
		t.Errorf("expected failed creating rotated daemon error, got %v", errFail)
	}
}

func TestUnifiedProcessPool_GetHasDaemonAndMarkDirty(t *testing.T) {
	var nilPool *UnifiedProcessPool
	if d, ok := nilPool.Get("kiosk"); ok || d != nil || nilPool.HasDaemon("kiosk") {
		t.Errorf("expected false/nil from nil UnifiedProcessPool")
	}
	nilPool.MarkDirty()

	pool := NewUnifiedProcessPool(PoolConfig{}, newPipeMockSpawner(pipeMockSpawnerOpts{pid: 99, initSessID: "00000000-0000-0000-0000-000000000099"}))
	defer pool.Close()
	if pool.HasDaemon("kiosk") {
		t.Errorf("expected false for unspawned target")
	}
	d, err := pool.GetOrCreate(context.Background(), "kiosk", "")
	if err != nil || !pool.HasDaemon("kiosk") {
		t.Fatalf("failed creating daemon: %v", err)
	}
	if got, ok := pool.Get("kiosk"); !ok || got != d {
		t.Errorf("expected pool.Get to return daemon %p, got %p, ok=%v", d, got, ok)
	}
	d.SetSessionID("00000000-0000-0000-0000-000000000098")
	if d.SessionID() != "00000000-0000-0000-0000-000000000098" {
		t.Errorf("expected session ID update")
	}
	pool.MarkDirty()
	if !d.IsDirty() {
		t.Errorf("expected daemon dirty after pool.MarkDirty()")
	}
	_ = d.Close()
	if _, ok := pool.Get("kiosk"); ok {
		t.Errorf("expected false for closed daemon")
	}
}

func TestUnifiedProcessPool_TargetModelsIgnored(t *testing.T) {
	var capturedModel string
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		pid:     200,
		onSpawn: func(cfg DaemonConfig) { capturedModel = cfg.Model },
	})
	pool := NewUnifiedProcessPool(PoolConfig{
		DefaultModel: "primary-model",
		TargetModels: map[string]string{"legacy-target": "overridden-model"},
	}, mock)
	defer pool.Close()
	daemon, err := pool.GetOrCreate(context.Background(), "legacy-target", "")
	if err != nil || daemon == nil || capturedModel != "primary-model" {
		t.Fatalf("unexpected TargetModelsIgnored: err=%v model=%q", err, capturedModel)
	}
}

type executeEphemeralCase struct {
	name, targetKey, prompt, spawnerOpt, echoRes, wantRes, wantErrSub string
	poolCfg                                                           PoolConfig
	isNilPool, isClosed, preCancel, wantErr                           bool
	timeout                                                           time.Duration
	wantErrIs                                                         error
}

func makeEphemeralSpawner(opt, echoRes string) DaemonSpawner {
	switch opt {
	case "echo":
		return newPipeMockSpawner(pipeMockSpawnerOpts{echoRes: echoRes})
	case "spawn_err":
		return newPipeMockSpawner(pipeMockSpawnerOpts{spawnErrOn: 1})
	case "stdin_closed":
		return newPipeMockSpawner(pipeMockSpawnerOpts{closeIn: true, pid: 203})
	default:
		return &MockDaemonSpawner{}
	}
}

func makeEphemeralContext(preCancel bool, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx := context.Background()
	if preCancel {
		cCtx, cancel := context.WithCancel(ctx)
		cancel()
		return cCtx, func() {}
	}
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return ctx, func() {}
}

func runExecuteEphemeralCase(t *testing.T, tt executeEphemeralCase) {
	t.Helper()
	spawner := makeEphemeralSpawner(tt.spawnerOpt, tt.echoRes)
	var pool *UnifiedProcessPool
	if !tt.isNilPool {
		pool = NewUnifiedProcessPool(tt.poolCfg, spawner)
		defer pool.Close()
		if tt.isClosed {
			_ = pool.Close()
		}
	}

	ctx, cancel := makeEphemeralContext(tt.preCancel, tt.timeout)
	defer cancel()

	res, err := pool.ExecuteEphemeral(ctx, tt.targetKey, tt.prompt)
	if tt.wantErr {
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if tt.wantErrIs != nil && !errors.Is(err, tt.wantErrIs) {
			t.Errorf("expected error %v, got %v", tt.wantErrIs, err)
		}
		if tt.wantErrSub != "" && !strings.Contains(err.Error(), tt.wantErrSub) {
			t.Errorf("expected error containing %q, got %v", tt.wantErrSub, err)
		}
		return
	}
	if err != nil || res != tt.wantRes {
		t.Fatalf("unexpected ExecuteEphemeral: res=%q want=%q err=%v", res, tt.wantRes, err)
	}
}

func TestUnifiedProcessPool_ExecuteEphemeral(t *testing.T) {
	tests := []executeEphemeralCase{
		{name: "success", targetKey: "ephemeral:classifier", prompt: "test prompt", poolCfg: PoolConfig{DefaultModel: "test-model"}, spawnerOpt: "echo", echoRes: "ephemeral result ok", wantRes: "ephemeral result ok"},
		{name: "nil pool error", targetKey: "key", prompt: "prompt", isNilPool: true, wantErr: true},
		{name: "empty targetKey error", targetKey: "", prompt: "prompt", wantErr: true, wantErrSub: "target key cannot be empty"},
		{name: "whitespace targetKey error", targetKey: "   ", prompt: "prompt", wantErr: true, wantErrSub: "target key cannot be empty"},
		{name: "context already cancelled", targetKey: "target", prompt: "prompt", spawnerOpt: "echo", preCancel: true, wantErr: true, wantErrIs: context.Canceled},
		{name: "context timeout awaiting result", targetKey: "target", prompt: "prompt", spawnerOpt: "mock", timeout: 20 * time.Millisecond, wantErr: true, wantErrIs: context.DeadlineExceeded},
		{name: "spawn failure", targetKey: "t1", prompt: "p1", spawnerOpt: "spawn_err", wantErr: true, wantErrSub: "daemon spawn failed"},
		{name: "closed pool failure", targetKey: "t2", prompt: "p2", spawnerOpt: "mock", isClosed: true, wantErr: true},
		{name: "stdin write failure", targetKey: "t3", prompt: "p3", spawnerOpt: "stdin_closed", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runExecuteEphemeralCase(t, tt)
		})
	}
}

func TestUnifiedProcessPool_EphemeralLLMFunc(t *testing.T) {
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{echoRes: "llm func reply"})
	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()
	llm := pool.EphemeralLLMFunc("target:classifier")
	if res, err := llm(context.Background(), "any-model", "test prompt"); err != nil || res != "llm func reply" {
		t.Fatalf("EphemeralLLMFunc failed: res=%q, err=%v", res, err)
	}
	var nilPool *UnifiedProcessPool
	if _, err := nilPool.EphemeralLLMFunc("target")(context.Background(), "model", "prompt"); err == nil {
		t.Fatalf("expected error from nil pool EphemeralLLMFunc")
	}
}

type geminiHomeDirCase struct {
	name             string
	createBlocker    bool
	wantErrSub       string
	wantEnvSanitized bool
}

func prepareGeminiHomeTarget(t *testing.T, tc geminiHomeDirCase) string {
	t.Helper()
	tmpDir := t.TempDir()
	if tc.createBlocker {
		targetHome := filepath.Join(tmpDir, "blocker-file")
		if err := os.WriteFile(targetHome, []byte("blocker"), 0o644); err != nil {
			t.Fatalf("failed to create blocker file: %v", err)
		}
		return targetHome
	}
	if tc.wantEnvSanitized {
		return filepath.Join(tmpDir, "runtime-home")
	}
	return ""
}

func assertGeminiHomeEnvSanitized(t *testing.T, targetHome string, capturedEnv []string, daemon *StreamingDaemon) {
	t.Helper()
	if info, statErr := os.Stat(filepath.Join(targetHome, ".gemini")); statErr != nil || !info.IsDir() {
		t.Fatalf("expected .gemini to exist as directory")
	}
	envStr := strings.Join(capturedEnv, "\n")
	expectedTurnEnv := filepath.Join(targetHome, ".gemini", "turn_env", "kiosk.sh")
	if !strings.Contains(envStr, "HOME="+targetHome) || !strings.Contains(envStr, "GEMINI_CLI_HOME="+targetHome) || !strings.Contains(envStr, "TZ=America/Los_Angeles") || !strings.Contains(envStr, "BASH_ENV="+expectedTurnEnv) {
		t.Errorf("expected HOME, GEMINI_CLI_HOME, TZ, BASH_ENV in daemon env, got %s", envStr)
	}
	if daemon.cfg.TurnEnvFile != expectedTurnEnv {
		t.Errorf("expected TurnEnvFile %q, got %q", expectedTurnEnv, daemon.cfg.TurnEnvFile)
	}
}

func runGeminiHomeDirCase(t *testing.T, tc geminiHomeDirCase) {
	t.Helper()
	targetHome := prepareGeminiHomeTarget(t, tc)
	var capturedEnv []string
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		onSpawn: func(cfg DaemonConfig) { capturedEnv = cfg.Env },
	})

	pool := NewUnifiedProcessPool(PoolConfig{
		GeminiHomeDir: targetHome,
		Env:           []string{"CUSTOM_A=1", "CUSTOM_B=2"},
		Timezone:      "America/Los_Angeles",
	}, mock)
	defer pool.Close()

	daemon, err := pool.GetOrCreate(context.Background(), "kiosk", "")
	if tc.wantErrSub != "" {
		if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
			t.Fatalf("expected error containing %q, got: %v", tc.wantErrSub, err)
		}
		return
	}
	if err != nil || daemon == nil {
		t.Fatalf("unexpected GetOrCreate error: %v", err)
	}
	if tc.wantEnvSanitized {
		assertGeminiHomeEnvSanitized(t, targetHome, capturedEnv, daemon)
	}
}

func TestUnifiedProcessPool_GeminiHomeDirInjection(t *testing.T) {
	t.Parallel()
	tests := []geminiHomeDirCase{
		{name: "CreateDirFailure", createBlocker: true, wantErrSub: "failed to create runtime home directory"},
		{name: "HomeDirInjectedSuccessfully", wantEnvSanitized: true},
		{name: "EmptyHomeDirIgnored"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runGeminiHomeDirCase(t, tc)
		})
	}
}

func TestUnifiedProcessPool_CloseConcurrentMultipleDaemons(t *testing.T) {
	pool := NewUnifiedProcessPool(PoolConfig{Model: "gemini-3.8-flash-low"}, newPipeMockSpawner(pipeMockSpawnerOpts{pid: 300}))
	ctx := context.Background()
	d1, err1 := pool.GetOrCreate(ctx, "target-a", "")
	d2, err2 := pool.GetOrCreate(ctx, "target-b", "")
	d3, err3 := pool.GetOrCreate(ctx, "target-c", "")
	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatalf("failed creating daemons: %v, %v, %v", err1, err2, err3)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("unexpected pool.Close() error: %v", err)
	}
	if d1.State() != StateClosed || d2.State() != StateClosed || d3.State() != StateClosed {
		t.Errorf("expected all daemons to be closed, got d1=%v, d2=%v, d3=%v", d1.State(), d2.State(), d3.State())
	}
}

type dirtyEvictionCase struct {
	name, initialKey                                                                          string
	prewarmed, updatePre                                                                      []string
	spawnerErrOn, expectSpawns                                                                int
	killErr                                                                                   error
	markDirty, setInflight, closeBeforeMarkDirty, expectEvicted, expectRewarmed, expectClosed bool
}

func setupDirtyEvictionInitialDaemon(ctx context.Context, pool *UnifiedProcessPool, tc dirtyEvictionCase) (*StreamingDaemon, error) {
	if tc.initialKey == "" {
		return nil, nil
	}
	initialDaemon, err := pool.GetOrCreate(ctx, tc.initialKey, "")
	if err != nil {
		return nil, err
	}
	if tc.setInflight {
		initialDaemon.inflightMu.Lock()
		initialDaemon.inflight = append(initialDaemon.inflight, &TurnContext{TurnID: "inflight-1"})
		initialDaemon.inflightMu.Unlock()
	}
	return initialDaemon, nil
}

func assertDirtyEvictionExpectations(t *testing.T, pool *UnifiedProcessPool, initialDaemon *StreamingDaemon, tc dirtyEvictionCase, spawnCount *atomic.Int32) {
	t.Helper()
	if initialDaemon != nil {
		if tc.expectClosed && initialDaemon.State() != StateClosed {
			t.Errorf("expected initial daemon closed, got %v", initialDaemon.State())
		}
		if !tc.expectClosed && initialDaemon.State() == StateClosed {
			t.Errorf("expected initial daemon NOT closed")
		}
	}
	if tc.expectEvicted {
		if _, ok := pool.Get(tc.initialKey); ok {
			t.Errorf("expected %q evicted", tc.initialKey)
		}
	}
	if tc.expectRewarmed {
		for _, target := range tc.prewarmed {
			if d, ok := pool.Get(target); !ok || d == nil || d == initialDaemon || d.State() == StateClosed {
				t.Errorf("expected prewarmed %q ready", target)
			}
		}
	}
	if tc.expectSpawns > 0 && int(spawnCount.Load()) != tc.expectSpawns {
		t.Errorf("expected %d total spawns, got %d", tc.expectSpawns, spawnCount.Load())
	}
}

func runDirtyEvictionCase(t *testing.T, tc dirtyEvictionCase) {
	t.Helper()
	if tc.closeBeforeMarkDirty {
		pool := NewUnifiedProcessPool(PoolConfig{}, &MockDaemonSpawner{})
		_ = pool.Close()
		pool.MarkDirty()
		return
	}

	var spawnCount atomic.Int32
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		killErr:    tc.killErr,
		spawnErrOn: tc.spawnerErrOn,
		onSpawn:    func(cfg DaemonConfig) { spawnCount.Add(1) },
	})

	pool := NewUnifiedProcessPool(PoolConfig{PrewarmedTargets: tc.prewarmed}, mock)
	defer pool.Close()

	ctx := context.Background()
	initialDaemon, err := setupDirtyEvictionInitialDaemon(ctx, pool, tc)
	if err != nil {
		t.Fatalf("failed creating initial daemon: %v", err)
	}

	if len(tc.updatePre) > 0 {
		pool.UpdatePrewarmedTargets(tc.updatePre)
	}
	if tc.markDirty {
		pool.MarkDirty()
		pool.WaitBackground()
	}

	assertDirtyEvictionExpectations(t, pool, initialDaemon, tc, &spawnCount)
}

func TestUnifiedProcessPool_DirtyEvictionAndReconciliation_Suite(t *testing.T) {
	t.Parallel()
	tests := []dirtyEvictionCase{
		{name: "IdlePrewarmedEvictedAndReplaced", initialKey: "kiosk", markDirty: true, prewarmed: []string{"kiosk"}, expectEvicted: false, expectRewarmed: true, expectClosed: true, expectSpawns: 2},
		{name: "IdleNonPrewarmedEvictedAndClosed", initialKey: "thread-nonprewarmed", markDirty: true, prewarmed: []string{"kiosk"}, expectEvicted: true, expectRewarmed: true, expectClosed: true, expectSpawns: 2},
		{name: "InflightPreservedDuringMarkDirty", initialKey: "thread-inflight", markDirty: true, setInflight: true, expectEvicted: false, expectClosed: false, expectSpawns: 1},
		{name: "EvictedDaemonCloseErrorLogged", initialKey: "thread-evict-err", markDirty: true, killErr: errors.New("simulated eviction kill error"), expectEvicted: true, expectClosed: true, expectSpawns: 1},
		{name: "RewarmingFailureLogged", initialKey: "kiosk", markDirty: true, prewarmed: []string{"kiosk"}, spawnerErrOn: 2, expectEvicted: true, expectRewarmed: false, expectClosed: true, expectSpawns: 2},
		{name: "ReconcilesNewlyAddedPrewarmedTargets", initialKey: "kiosk", markDirty: true, prewarmed: []string{"kiosk"}, updatePre: []string{"touch-kiosk-kitchen"}, expectEvicted: true, expectClosed: true, expectSpawns: 2},
		{name: "ClosedPoolMarkDirtyReturnsEarly", closeBeforeMarkDirty: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDirtyEvictionCase(t, tc)
		})
	}
}

func TestUnifiedProcessPool_InflightRotatedOnCompletion(t *testing.T) {
	t.Parallel()
	var spawnCounter atomic.Int32
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		onSpawn: func(cfg DaemonConfig) { spawnCounter.Add(1) },
	})
	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	d1, err := pool.GetOrCreate(context.Background(), "thread-inflight-rot", "")
	if err != nil {
		t.Fatalf("failed creating initial daemon: %v", err)
	}
	d1.inflight = append(d1.inflight, &TurnContext{TurnID: "t-inflight", Sink: newMockTurnSink(), CreatedAt: time.Now()})
	pool.MarkDirty()
	if d1.State() == StateClosed || !d1.IsDirty() {
		t.Fatalf("expected in-flight daemon marked dirty and not closed")
	}
	d1.dispatchNDJSONLine(`{"event":"result","result":{"status":"SUCCESS","response":"finished"}}`)
	if should, _ := pool.ShouldRotate(d1); !should {
		t.Fatalf("expected dirty daemon to indicate rotation needed after turn completion")
	}
	rotatedSess, err := pool.RotateDaemon(context.Background(), "thread-inflight-rot", "")
	if err != nil || rotatedSess == nil || rotatedSess == d1 || d1.State() != StateClosed || spawnCounter.Load() != 2 {
		t.Fatalf("rotation failed: sess=%v err=%v d1State=%v count=%d", rotatedSess, err, d1.State(), spawnCounter.Load())
	}
	if got := (&unifiedSessionLease{sessionID: "fallback-id"}).SessionID(); got != "fallback-id" {
		t.Fatalf("expected fallback session ID, got %q", got)
	}
	_ = pool.Close()
	if _, err := pool.RotateDaemon(context.Background(), "thread-inflight-rot", ""); err == nil {
		t.Fatalf("expected RotateDaemon error on closed pool")
	}
}

func TestUnifiedProcessPool_MarkDirty_SingleflightBarrier(t *testing.T) {
	t.Parallel()
	var concurrentSpawns, maxConcurrentSpawns, totalSpawns atomic.Int32
	spawnGate := make(chan struct{})

	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		onSpawn: func(cfg DaemonConfig) {
			cur := concurrentSpawns.Add(1)
			defer concurrentSpawns.Add(-1)
			for {
				oldMax := maxConcurrentSpawns.Load()
				if cur <= oldMax || maxConcurrentSpawns.CompareAndSwap(oldMax, cur) {
					break
				}
			}
			if totalSpawns.Add(1) > 1 {
				<-spawnGate
			}
		},
	})

	pool := NewUnifiedProcessPool(PoolConfig{PrewarmedTargets: []string{"kiosk"}}, mock)
	defer pool.Close()

	d1, err := pool.GetOrCreate(context.Background(), "kiosk", "")
	if err != nil || d1.InflightCount() != 0 {
		t.Fatalf("failed creating initial daemon: %v", err)
	}
	for range 10 {
		pool.MarkDirty()
	}
	close(spawnGate)
	pool.WaitBackground()
	d2, ok := pool.Get("kiosk")
	if !ok || d2 == nil || d2 == d1 || d2.State() == StateClosed {
		t.Fatalf("expected kiosk daemon replacement")
	}
	if maxConcurrentSpawns.Load() > 1 || totalSpawns.Load() != 2 {
		t.Errorf("spawn concurrency check failed: max=%d total=%d", maxConcurrentSpawns.Load(), totalSpawns.Load())
	}
}

func TestUnifiedProcessPool_GetOrCreate_RejectsDirtyDaemon(t *testing.T) {
	tests := []struct {
		name         string
		killErr      error
		expectSpawns int
	}{
		{name: "SpawnsFreshAndClosesDirty", expectSpawns: 2},
		{name: "DirtyDaemonCloseErrorLogged", killErr: errors.New("simulated dirty daemon kill error"), expectSpawns: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var spawnCounter atomic.Int32
			mock := newPipeMockSpawner(pipeMockSpawnerOpts{
				killErr: tc.killErr,
				onSpawn: func(cfg DaemonConfig) { spawnCounter.Add(1) },
			})

			pool := NewUnifiedProcessPool(PoolConfig{}, mock)
			defer pool.Close()

			ctx := context.Background()
			d1, err := pool.GetOrCreate(ctx, "thread-dirty-target", "")
			if err != nil {
				t.Fatalf("initial GetOrCreate failed: %v", err)
			}

			d1.MarkDirty()
			d2, err := pool.GetOrCreate(ctx, "thread-dirty-target", "")
			if err != nil || d2 == d1 || d2.IsDirty() {
				t.Fatalf("second GetOrCreate failed: d1=%p d2=%p err=%v", d1, d2, err)
			}

			pool.WaitBackground()
			if d1.State() != StateClosed || (tc.expectSpawns > 0 && int(spawnCounter.Load()) != tc.expectSpawns) {
				t.Errorf("rejected dirty daemon check failed: d1State=%v spawns=%d", d1.State(), spawnCounter.Load())
			}
		})
	}
}

type prewarmedTargetsUpdateCase struct {
	name          string
	nilPool       bool
	initial       []string
	update        []string
	mutateFirst   bool
	expectedSlice []string
}

func assertPrewarmedTargetsMatches(t *testing.T, pool *UnifiedProcessPool, expectedSlice []string) {
	t.Helper()
	targets := pool.PrewarmedTargets()
	if len(targets) != len(expectedSlice) {
		t.Fatalf("expected len %d, got %v", len(expectedSlice), targets)
	}
	for i, exp := range expectedSlice {
		if targets[i] != exp {
			t.Errorf("target[%d] expected %q, got %q", i, exp, targets[i])
		}
	}
}

func runPrewarmedTargetsUpdateCase(t *testing.T, tc prewarmedTargetsUpdateCase) {
	t.Helper()
	if tc.nilPool {
		var p *UnifiedProcessPool
		if got := p.PrewarmedTargets(); got != nil {
			t.Errorf("expected nil, got %v", got)
		}
		p.UpdatePrewarmedTargets(tc.update)
		return
	}
	pool := NewUnifiedProcessPool(PoolConfig{PrewarmedTargets: tc.initial}, &MockDaemonSpawner{})
	defer pool.Close()
	if tc.mutateFirst {
		targets := pool.PrewarmedTargets()
		if len(targets) != len(tc.expectedSlice) {
			t.Fatalf("unexpected len: %v", targets)
		}
		targets[0] = "mutated"
		if fresh := pool.PrewarmedTargets(); fresh[0] != tc.expectedSlice[0] {
			t.Errorf("defensive copy violated: %q", fresh[0])
		}
		return
	}
	pool.UpdatePrewarmedTargets(tc.update)
	assertPrewarmedTargetsMatches(t, pool, tc.expectedSlice)
	if tc.name == "UpdateFiltersWhitespaceAndEmpty" {
		pool.UpdatePrewarmedTargets(nil)
		if got := pool.PrewarmedTargets(); got != nil {
			t.Errorf("expected nil targets, got %v", got)
		}
	}
}

func TestUnifiedProcessPool_PrewarmedTargets_DynamicUpdate(t *testing.T) {
	tests := []prewarmedTargetsUpdateCase{
		{name: "NilPool_ReturnsNil", nilPool: true, update: []string{"test"}},
		{name: "InitialAndDefensiveCopy", initial: []string{"a", "b"}, mutateFirst: true, expectedSlice: []string{"a", "b"}},
		{name: "UpdateFiltersWhitespaceAndEmpty", update: []string{"  target1  ", "", "   ", "target2"}, expectedSlice: []string{"target1", "target2"}},
		{name: "UpdateDeduplicatesTargets", update: []string{"kiosk", "kiosk", "touch-kiosk-kitchen", "  kiosk  "}, expectedSlice: []string{"kiosk", "touch-kiosk-kitchen"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runPrewarmedTargetsUpdateCase(t, tc)
		})
	}
}
