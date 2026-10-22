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

func newPipeMockSpawner(opts pipeMockSpawnerOpts) *MockDaemonSpawner {
	var count, attemptCount atomic.Int32
	return &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			idx := count.Add(1)
			if opts.onSpawn != nil {
				opts.onSpawn(cfg)
			}
			if (opts.failSpawn != nil && opts.failSpawn.Load()) || (opts.spawnErrOn > 0 && int(idx) == opts.spawnErrOn) {
				return nil, nil, nil, nil, errors.New("daemon spawn failed")
			}
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			if opts.closeIn {
				_ = inR.Close()
				_ = inW.Close()
			}

			sessID := cfg.SessionID
			if sessID == "" {
				if idx > 1 || opts.initSessID == "" {
					sessID = fmt.Sprintf("00000000-0000-0000-0000-%012d", idx)
				} else {
					sessID = opts.initSessID
				}
			}

			go func() {
				defer outW.Close()
				if opts.skipInitOnAttempt == 0 || int(idx) != opts.skipInitOnAttempt {
					_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":%q}`+"\n", sessID)))
				}
				buf := make([]byte, 4096)
				for {
					n, err := inR.Read(buf)
					if err != nil {
						return
					}
					line := string(buf[:n])
					if opts.onUser != nil && strings.Contains(line, `"event":"user"`) {
						att := int(attemptCount.Add(1))
						resp, errStr, status := opts.onUser(att, line)
						if status == "" {
							if errStr != "" && resp == "" {
								status = "ERROR"
							} else {
								status = "SUCCESS"
							}
						}
						_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"result","response":%q,"error":%q,"status":%q}`+"\n", resp, errStr, status)))
					} else if opts.echoRes != "" && strings.Contains(line, `"event":"user"`) {
						_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"result","result":{"status":"SUCCESS","response":%q}}`+"\n", opts.echoRes)))
					}
				}
			}()

			pid := opts.pid
			if pid == 0 {
				pid = int(idx * 100)
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

func TestUnifiedProcessPool_ModelResolution(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, model, defaultModel, wantModel string
		checkDaemon                          bool
	}{
		{name: "ModelBound", model: "gemini-3.8-flash-low", wantModel: "gemini-3.8-flash-low", checkDaemon: true},
		{name: "ModelFallback", defaultModel: "gemini-2.5-pro", wantModel: "gemini-2.5-pro"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mock := NewMockSpawner()
			pool := NewUnifiedProcessPool(PoolConfig{Model: tc.model, DefaultModel: tc.defaultModel}, mock)
			defer pool.Close()
			if got := pool.cfg.Model; got != tc.wantModel {
				t.Fatalf("expected pool model %q, got %q", tc.wantModel, got)
			}
			if tc.checkDaemon {
				daemon, err := pool.GetOrCreate(context.Background(), "target-1", "")
				if err != nil || daemon == nil {
					t.Fatalf("unexpected GetOrCreate result: daemon=%v err=%v", daemon, err)
				}
				mock.mu.Lock()
				spawnCfg := mock.lastSpawnCfg
				mock.mu.Unlock()
				if spawnCfg.Model != tc.wantModel {
					t.Fatalf("expected model %s, got %s", tc.wantModel, spawnCfg.Model)
				}
			}
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
		for i := 0; i < turns; i++ {
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

func TestUnifiedProcessPool_ExecuteEphemeral(t *testing.T) {
	tests := []struct {
		name, targetKey, prompt, spawnerOpt, echoRes, wantRes, wantErrSub string
		poolCfg                                                           PoolConfig
		isNilPool, isClosed, preCancel, wantErr                           bool
		timeout                                                           time.Duration
		wantErrIs                                                         error
	}{
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
			var spawner DaemonSpawner
			switch tt.spawnerOpt {
			case "echo":
				spawner = newPipeMockSpawner(pipeMockSpawnerOpts{echoRes: tt.echoRes})
			case "spawn_err":
				spawner = newPipeMockSpawner(pipeMockSpawnerOpts{spawnErrOn: 1})
			case "stdin_closed":
				spawner = newPipeMockSpawner(pipeMockSpawnerOpts{closeIn: true, pid: 203})
			default:
				spawner = &MockDaemonSpawner{}
			}

			var pool *UnifiedProcessPool
			if !tt.isNilPool {
				pool = NewUnifiedProcessPool(tt.poolCfg, spawner)
				defer pool.Close()
				if tt.isClosed {
					_ = pool.Close()
				}
			}

			ctx := context.Background()
			if tt.preCancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			} else if tt.timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.timeout)
				defer cancel()
			}

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

func TestUnifiedProcessPool_GeminiHomeDirInjection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		createBlocker    bool
		wantErrSub       string
		wantEnvSanitized bool
	}{
		{name: "CreateDirFailure", createBlocker: true, wantErrSub: "failed to create runtime home directory"},
		{name: "HomeDirInjectedSuccessfully", wantEnvSanitized: true},
		{name: "EmptyHomeDirIgnored"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			var targetHome string
			if tc.createBlocker {
				targetHome = filepath.Join(tmpDir, "blocker-file")
				if err := os.WriteFile(targetHome, []byte("blocker"), 0o644); err != nil {
					t.Fatalf("failed to create blocker file: %v", err)
				}
			} else if tc.wantEnvSanitized {
				targetHome = filepath.Join(tmpDir, "runtime-home")
			}

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
				if info, statErr := os.Stat(filepath.Join(targetHome, ".gemini")); statErr != nil || !info.IsDir() {
					t.Fatalf("expected .gemini to exist as directory")
				}
				envStr := strings.Join(capturedEnv, "\n")
				if !strings.Contains(envStr, "HOME="+targetHome) || !strings.Contains(envStr, "GEMINI_CLI_HOME="+targetHome) || !strings.Contains(envStr, "TZ=America/Los_Angeles") {
					t.Errorf("expected HOME, GEMINI_CLI_HOME, TZ in daemon env, got %s", envStr)
				}
			}
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

func TestUnifiedProcessPool_DirtyEvictionAndReconciliation_Suite(t *testing.T) {
	t.Parallel()
	type testCase struct {
		name, initialKey                                                                          string
		prewarmed, updatePre                                                                      []string
		spawnerErrOn, expectSpawns                                                                int
		killErr                                                                                   error
		markDirty, setInflight, closeBeforeMarkDirty, expectEvicted, expectRewarmed, expectClosed bool
	}

	tests := []testCase{
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
			var initialDaemon *StreamingDaemon
			if tc.initialKey != "" {
				var err error
				if initialDaemon, err = pool.GetOrCreate(ctx, tc.initialKey, ""); err != nil {
					t.Fatalf("failed creating initial daemon: %v", err)
				}
				if tc.setInflight {
					initialDaemon.inflightMu.Lock()
					initialDaemon.inflight = append(initialDaemon.inflight, &TurnContext{TurnID: "inflight-1"})
					initialDaemon.inflightMu.Unlock()
				}
			}

			if len(tc.updatePre) > 0 {
				pool.UpdatePrewarmedTargets(tc.updatePre)
			}
			if tc.markDirty {
				pool.MarkDirty()
				pool.WaitBackground()
			}

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
	for i := 0; i < 10; i++ {
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

func TestUnifiedProcessPool_PrewarmedTargets_DynamicUpdate(t *testing.T) {
	tests := []struct {
		name          string
		nilPool       bool
		initial       []string
		update        []string
		mutateFirst   bool
		expectedSlice []string
	}{
		{name: "NilPool_ReturnsNil", nilPool: true, update: []string{"test"}},
		{name: "InitialAndDefensiveCopy", initial: []string{"a", "b"}, mutateFirst: true, expectedSlice: []string{"a", "b"}},
		{name: "UpdateFiltersWhitespaceAndEmpty", update: []string{"  target1  ", "", "   ", "target2"}, expectedSlice: []string{"target1", "target2"}},
		{name: "UpdateDeduplicatesTargets", update: []string{"kiosk", "kiosk", "touch-kiosk-kitchen", "  kiosk  "}, expectedSlice: []string{"kiosk", "touch-kiosk-kitchen"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
			targets := pool.PrewarmedTargets()
			if len(targets) != len(tc.expectedSlice) {
				t.Fatalf("expected len %d, got %v", len(tc.expectedSlice), targets)
			}
			for i, exp := range tc.expectedSlice {
				if targets[i] != exp {
					t.Errorf("target[%d] expected %q, got %q", i, exp, targets[i])
				}
			}
			if tc.name == "UpdateFiltersWhitespaceAndEmpty" {
				pool.UpdatePrewarmedTargets(nil)
				if got := pool.PrewarmedTargets(); got != nil {
					t.Errorf("expected nil targets, got %v", got)
				}
			}
		})
	}
}

func TestUnifiedProcessPool_SessionPropagation_Suite(t *testing.T) {
	tests := []struct {
		name, inputSessID, secondSessID, expectCapturedSess, expectSessID string
		initialTurns, expectSpawns                                        int
		expectSecondDiff                                                  bool
	}{
		{name: "PropagatesCustomSessionID", inputSessID: "11111111-2222-3333-4444-555555555555", secondSessID: "11111111-2222-3333-4444-555555555555", expectCapturedSess: "11111111-2222-3333-4444-555555555555", expectSessID: "11111111-2222-3333-4444-555555555555", expectSpawns: 1},
		{name: "FreshSessionOnEmptyID", inputSessID: "", expectCapturedSess: "", expectSessID: "00000000-0000-0000-0000-000000000001", expectSpawns: 1},
		{name: "MismatchedSessionIDRotates", inputSessID: "11111111-1111-1111-1111-111111111111", secondSessID: "22222222-2222-2222-2222-222222222222", expectCapturedSess: "11111111-1111-1111-1111-111111111111", expectSessID: "11111111-1111-1111-1111-111111111111", expectSecondDiff: true, expectSpawns: 2},
		{name: "PropagatesInitialTurnCount", inputSessID: "44444444-4444-4444-4444-444444444444", initialTurns: 5, expectCapturedSess: "44444444-4444-4444-4444-444444444444", expectSessID: "44444444-4444-4444-4444-444444444444", expectSpawns: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedCfg DaemonConfig
			var spawnCount atomic.Int32
			mock := newPipeMockSpawner(pipeMockSpawnerOpts{
				onSpawn: func(cfg DaemonConfig) {
					spawnCount.Add(1)
					capturedCfg = cfg
				},
			})
			pool := NewUnifiedProcessPool(PoolConfig{}, mock)
			defer pool.Close()

			ctx := context.Background()
			var daemon *StreamingDaemon
			var err error
			if tc.initialTurns > 0 {
				daemon, err = pool.GetOrCreate(ctx, "target-prop", tc.inputSessID, tc.initialTurns)
			} else {
				daemon, err = pool.GetOrCreate(ctx, "target-prop", tc.inputSessID)
			}
			if err != nil {
				t.Fatalf("GetOrCreate failed: %v", err)
			}

			if capturedCfg.SessionID != tc.expectCapturedSess || daemon.SessionID() != tc.expectSessID {
				t.Errorf("session ID check failed: captured=%q daemon=%q", capturedCfg.SessionID, daemon.SessionID())
			}
			if tc.initialTurns > 0 && daemon.TurnCount() != tc.initialTurns {
				t.Errorf("daemon TurnCount expected %d, got %d", tc.initialTurns, daemon.TurnCount())
			}
			if tc.secondSessID != "" {
				d2, err2 := pool.GetOrCreate(ctx, "target-prop", tc.secondSessID)
				if err2 != nil || (tc.expectSecondDiff && d2 == daemon) || (!tc.expectSecondDiff && d2 != daemon) {
					t.Fatalf("second call mismatch: d2=%p daemon=%p err=%v", d2, daemon, err2)
				}
			}
			if int(spawnCount.Load()) != tc.expectSpawns {
				t.Errorf("expected %d total spawns, got %d", tc.expectSpawns, spawnCount.Load())
			}
		})
	}
}

func TestUnifiedProcessPool_AcquireLease_Suite(t *testing.T) {
	tests := []struct {
		name, customExistingSessID, respPayload, respErr, respStatus, expectPrevSess, expectSubstantive                          string
		existingTurnCount, maxTurns, expectTurnCount                                                                             int
		targetLockTimeout                                                                                                        time.Duration
		onRotatedErr                                                                                                             error
		validationNilPool, validationEmptyTarget, twoAttemptsThrottle, concurrencyConflict                                       bool
		expectAcquireErr, expectExecErr, expectReleaseErr, expectRotated, expectColdBefore, expectColdAfter, expectPreservedSess bool
	}{
		{name: "BasicExecution", customExistingSessID: "550e8400-e29b-41d4-a716-446655440001", expectPrevSess: "550e8400-e29b-41d4-a716-446655440000", respPayload: "lease reply", expectColdBefore: true, expectColdAfter: false, expectTurnCount: 1, expectSubstantive: "lease reply"},
		{name: "RotationAndPersistence", maxTurns: 1, respPayload: "ok", expectRotated: true},
		{name: "PersistenceFailureRollback", maxTurns: 1, respPayload: "ok", onRotatedErr: errors.New("simulated DB latch timeout"), expectReleaseErr: true},
		{name: "ConcurrencySerialization", targetLockTimeout: 200 * time.Millisecond, respPayload: "ok", concurrencyConflict: true},
		{name: "CancelRotationOnQuotaExhausted", maxTurns: 1, respErr: "RESOURCE_EXHAUSTED: quota exceeded. Resets in 30s.", respStatus: "ERROR", expectExecErr: true, expectRotated: false},
		{name: "RotatesWhenResponseSubstantiveDespiteStderrThrottle", maxTurns: 1, respPayload: "Task completed successfully and verified.", respErr: "API error (attempt 1): RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.", respStatus: "DONE", expectRotated: true, expectSubstantive: "Task completed successfully and verified."},
		{name: "ClearsCancelRotationOnSuccessfulRetry", maxTurns: 1, twoAttemptsThrottle: true, expectRotated: true, expectSubstantive: "Retry succeeded with substantive output."},
		{name: "ConversationalRateLimitTextDoesNotCancelRotation", maxTurns: 1, respPayload: "I hit an external rate limit reached earlier, but I cached the result.", respStatus: "DONE", expectRotated: true, expectSubstantive: "rate limit reached"},
		{name: "Validation_NilPool", validationNilPool: true, expectAcquireErr: true},
		{name: "Validation_EmptyTarget", validationEmptyTarget: true, expectAcquireErr: true},
		{name: "PropagatesInitialTurnCountAndRotates", existingTurnCount: DefaultMaxSessionTurns - 1, respPayload: "ok", expectRotated: true, expectTurnCount: DefaultMaxSessionTurns},
		{name: "DefersRotationToTurnEndEvenIfExceeded", existingTurnCount: DefaultMaxSessionTurns, respPayload: "ok", expectRotated: true, expectPreservedSess: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			if tc.validationNilPool {
				var nilPool *UnifiedProcessPool
				if _, err := nilPool.AcquireLease(ctx, "target"); err == nil {
					t.Error("expected error from nil pool AcquireLease")
				}
				return
			}
			if tc.validationEmptyTarget {
				pool := NewUnifiedProcessPool(PoolConfig{}, &MockDaemonSpawner{})
				defer pool.Close()
				if _, err := pool.AcquireLease(ctx, "   "); err == nil {
					t.Error("expected error from empty targetKey AcquireLease")
				}
				return
			}

			var rotationCalled bool
			var rotatedOld, rotatedNew string
			pinnedSessID := tc.customExistingSessID
			if pinnedSessID == "" {
				pinnedSessID = uuid.New().String()
			}

			mock := newPipeMockSpawner(pipeMockSpawnerOpts{
				pid:        350,
				initSessID: pinnedSessID,
				onUser: func(attempt int, line string) (string, string, string) {
					if tc.twoAttemptsThrottle {
						if attempt == 1 {
							return "", "RESOURCE_EXHAUSTED: quota exceeded. Resets in 5s.", "ERROR"
						}
						return "Retry succeeded with substantive output.", "", "DONE"
					}
					return tc.respPayload, tc.respErr, tc.respStatus
				},
			})

			poolCfg := PoolConfig{
				MaxSessionTurns:   tc.maxTurns,
				TargetLockTimeout: tc.targetLockTimeout,
				GetSessionRecord: func(ctx context.Context, targetKey string) (SessionRecord, error) {
					return SessionRecord{ActiveSessionID: pinnedSessID, PreviousSessionID: tc.expectPrevSess, TurnCount: tc.existingTurnCount}, nil
				},
				OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
					rotationCalled = true
					rotatedOld, rotatedNew = oldSessionID, newSessionID
					return tc.onRotatedErr
				},
			}

			pool := NewUnifiedProcessPool(poolCfg, mock)
			defer pool.Close()

			targetKey := "thread-lease-" + uuid.New().String()
			lease, err := pool.AcquireLease(ctx, targetKey)
			if tc.expectAcquireErr {
				if err == nil {
					t.Fatalf("expected AcquireLease error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected AcquireLease error: %v", err)
			}
			if tc.expectColdBefore && !lease.IsCold() {
				t.Errorf("expected IsCold before execution to be true")
			}
			if tc.expectPrevSess != "" && lease.PreviousSessionID() != tc.expectPrevSess {
				t.Errorf("expected prev session %q, got %q", tc.expectPrevSess, lease.PreviousSessionID())
			}
			if tc.expectPreservedSess && lease.SessionID() != pinnedSessID {
				t.Errorf("expected session %q, got %q", pinnedSessID, lease.SessionID())
			}
			if tc.expectPreservedSess && rotationCalled {
				t.Errorf("rotation must NOT be called before release")
			}

			if tc.concurrencyConflict {
				ctxTimeout, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
				defer cancel()
				if _, cErr := pool.AcquireLease(ctxTimeout, targetKey); cErr == nil {
					t.Fatalf("expected concurrent AcquireLease timeout")
				}
			}

			turnCtx := &TurnContext{TurnID: "turn-1", Prompt: "prompt-1", Ctx: ctx}
			res, execErr := lease.Execute(ctx, turnCtx)
			if tc.twoAttemptsThrottle {
				if execErr == nil {
					t.Fatal("expected attempt 1 to fail on throttle")
				}
				turnCtx2 := &TurnContext{TurnID: "turn-1-retry", Prompt: "attempt 2", Ctx: ctx}
				if res, execErr = lease.Execute(ctx, turnCtx2); execErr != nil {
					t.Fatalf("attempt 2 failed: %v", execErr)
				}
			}

			if tc.expectExecErr {
				if execErr == nil {
					t.Fatalf("expected Execute error, got nil")
				}
			} else if execErr != nil {
				t.Fatalf("unexpected Execute error: %v", execErr)
			}
			if tc.expectSubstantive != "" && (res == nil || !strings.Contains(res.Response, tc.expectSubstantive)) {
				t.Errorf("expected response containing %q, got: %+v", tc.expectSubstantive, res)
			}
			if tc.expectColdAfter && lease.IsCold() {
				t.Errorf("expected IsCold to be false after execution")
			}
			if tc.expectTurnCount > 0 && lease.TurnCount() != tc.expectTurnCount {
				t.Errorf("expected TurnCount %d, got %d", tc.expectTurnCount, lease.TurnCount())
			}

			relErr := lease.Release()
			if tc.expectReleaseErr {
				if relErr == nil {
					t.Fatalf("expected Release error, got nil")
				}
				if tc.onRotatedErr != nil && !errors.Is(relErr, tc.onRotatedErr) {
					t.Errorf("expected error %v, got %v", tc.onRotatedErr, relErr)
				}
				if pool.HasDaemon(targetKey) {
					t.Errorf("expected daemon evicted")
				}
				return
			}
			if relErr != nil {
				t.Fatalf("unexpected Release error: %v", relErr)
			}

			if tc.concurrencyConflict {
				if lease3, err3 := pool.AcquireLease(ctx, targetKey); err3 != nil {
					t.Fatalf("lease3 error: %v", err3)
				} else {
					_ = lease3.Release()
				}
			}

			if tc.expectRotated {
				if !rotationCalled || rotatedOld != pinnedSessID || rotatedNew == "" || rotatedNew == rotatedOld {
					t.Errorf("expected valid rotation: called=%v old=%q new=%q", rotationCalled, rotatedOld, rotatedNew)
				}
			} else if rotationCalled {
				t.Errorf("expected rotation NOT to be called")
			}
		})
	}
}

func TestUnifiedProcessPool_LeaseTurnSinkWrapper_AllMethods(t *testing.T) {
	inner := newMockTurnSink()
	buf := NewBufferingTurnSink(BufferingTurnSinkConfig{})
	wrapper := &leaseTurnSinkWrapper{inner: inner, buf: buf}

	wrapper.OnTurnStarted()
	wrapper.OnThinking()
	wrapper.OnStepStarted(1)
	wrapper.OnToolCall("bash", "ls")
	wrapper.OnToolCompleted("bash", "native", 10*time.Millisecond, "ok")
	wrapper.OnSkillActivated("self-improvement", "discord")
	wrapper.OnTextDelta("delta")
	res := &TurnResult{Response: "ok"}
	wrapper.OnResult(res)
	testErr := errors.New("test error")
	wrapper.OnError(testErr)

	if !inner.started || inner.thinking != 1 {
		t.Errorf("expected inner.started and inner.thinking == 1")
	}

	nilWrapper := &leaseTurnSinkWrapper{inner: nil, buf: NewBufferingTurnSink(BufferingTurnSinkConfig{})}
	nilWrapper.OnTurnStarted()
	nilWrapper.OnThinking()
	nilWrapper.OnStepStarted(1)
	nilWrapper.OnToolCall("bash", "ls")
	nilWrapper.OnToolCompleted("bash", "native", 10*time.Millisecond, "ok")
	nilWrapper.OnSkillActivated("self-improvement", "discord")
	nilWrapper.OnTextDelta("delta")
	nilWrapper.OnResult(res)
	nilWrapper.OnError(testErr)
}

func TestUnifiedProcessPool_SetSessionCallbacks(t *testing.T) {
	pool := NewUnifiedProcessPool(PoolConfig{}, nil)
	defer pool.Close()
	getRec := func(ctx context.Context, targetKey string) (SessionRecord, error) {
		return SessionRecord{ActiveSessionID: "s1"}, nil
	}
	onRot := func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error { return nil }
	pool.SetSessionCallbacks(getRec, onRot)
	if pool.cfg.GetSessionRecord == nil || pool.cfg.OnSessionRotated == nil {
		t.Fatalf("expected callbacks to be registered on pool")
	}
	var nilPool *UnifiedProcessPool
	nilPool.SetSessionCallbacks(getRec, onRot)
}

func TestUnifiedProcessPool_VanadisChunkIDEnvAndRefresh(t *testing.T) {
	t.Parallel()
	filteredEnv := filterVanadisChunkEnv([]string{"NO_EQUALS_ENTRY", "AERIAL_VANADIS_CHUNK_ID=stale", "VANADIS_CHUNK_ID=stale2", "KEEP_ME=1"})
	if len(filteredEnv) != 2 || filteredEnv[0] != "NO_EQUALS_ENTRY" || filteredEnv[1] != "KEEP_ME=1" {
		t.Fatalf("unexpected filteredEnv: %v", filteredEnv)
	}

	throwaway := NewThrowawayTurnSink()
	throwaway.OnTurnStarted()
	throwaway.OnThinking()
	throwaway.OnToolCall("tool", "cmd")
	throwaway.OnToolCompleted("tool", "srv", 0, "ok")
	throwaway.OnSkillActivated("skill", "src")
	throwaway.OnTextDelta("delta")
	throwaway.OnResult(&TurnResult{Response: "ok"})
	throwaway.OnError(errors.New("err"))

	var mu sync.Mutex
	var spawnedEnvs [][]string
	var spawnedSessions []string
	pinnedSessID := uuid.New().String()
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		onSpawn: func(cfg DaemonConfig) {
			mu.Lock()
			spawnedEnvs = append(spawnedEnvs, append([]string(nil), cfg.Env...))
			spawnedSessions = append(spawnedSessions, cfg.SessionID)
			mu.Unlock()
		},
		onUser: func(attempt int, line string) (string, string, string) { return "ok", "", "SUCCESS" },
	})

	pool := NewUnifiedProcessPool(PoolConfig{
		GeminiHomeDir: t.TempDir(),
		Env:           []string{"AERIAL_VANADIS_CHUNK_ID=stale-parent", "BASE_VAR=1"},
		GetSessionRecord: func(ctx context.Context, targetKey string) (SessionRecord, error) {
			return SessionRecord{ActiveSessionID: pinnedSessID, PreviousSessionID: "prev-sess-1", TurnCount: 0}, nil
		},
	}, mock)
	defer pool.Close()

	ctx := context.Background()
	lease0, err := pool.AcquireLease(ctx, "thread-vanadis")
	if err != nil || lease0.IsEphemeral() || lease0.PreviousSessionID() != "prev-sess-1" || !lease0.IsCold() || lease0.TurnCount() != 0 {
		t.Fatalf("unexpected lease0: err=%v", err)
	}
	if _, err := lease0.Execute(ctx, nil); err == nil {
		t.Fatalf("expected error for nil TurnContext")
	}
	if _, err := lease0.Execute(ctx, &TurnContext{TurnID: "t0", Prompt: "warmup", ChannelInstructions: "chan-rules", ThreadSummary: "summary-text", CoordinationContext: "coord-text"}); err != nil {
		t.Fatalf("Execute 0 error: %v", err)
	}
	_ = lease0.Release()

	for i, chunk := range []string{"chunk 1", "chunk 2"} {
		l, err := pool.AcquireLease(ctx, "thread-vanadis")
		if err != nil || (i == 0 && l.SessionID() != pinnedSessID) {
			t.Fatalf("AcquireLease %d error: %v", i+1, err)
		}
		if _, err := l.Execute(ctx, &TurnContext{TurnID: fmt.Sprintf("t%d", i+1), Prompt: chunk, Ctx: ctx}); err != nil {
			t.Fatalf("Execute %d error: %v", i+1, err)
		}
		_ = l.Release()
	}

	d3, err := pool.GetOrCreate(ctx, "thread-vanadis", "")
	if err != nil || d3.SessionID() != pinnedSessID {
		t.Fatalf("GetOrCreate 3 failed: err=%v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(spawnedEnvs) != 1 || strings.Contains(strings.Join(spawnedEnvs[0], "\n"), "AERIAL_VANADIS_CHUNK_ID=") || spawnedSessions[0] != pinnedSessID {
		t.Fatalf("spawn env/session validation failed: envs=%v sess=%v", spawnedEnvs, spawnedSessions)
	}

	var retryAttempts atomic.Int32
	retryMock := newPipeMockSpawner(pipeMockSpawnerOpts{
		pid:               700,
		initSessID:        pinnedSessID,
		skipInitOnAttempt: 1,
		onSpawn:           func(cfg DaemonConfig) { retryAttempts.Add(1) },
	})
	retryPool := NewUnifiedProcessPool(PoolConfig{HandshakeTimeout: 5 * time.Millisecond}, retryMock)
	defer retryPool.Close()
	if dRetry, rErr := retryPool.GetOrCreate(ctx, "thread-handshake-retry", ""); rErr != nil || dRetry == nil || retryAttempts.Load() != 2 {
		t.Fatalf("expected handshake timeout retry: d=%v err=%v attempts=%d", dRetry, rErr, retryAttempts.Load())
	}
}

func TestUnifiedProcessPool_PrewarmAndLeaseResilience(t *testing.T) {
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		pid:    900,
		onUser: func(att int, line string) (string, string, string) { return "ok", "", "SUCCESS" },
	})
	pool := NewUnifiedProcessPool(PoolConfig{
		GeminiHomeDir:    t.TempDir(),
		PrewarmedTargets: []string{"discord:worker-0", "discord:worker-1"},
	}, mock)
	defer pool.Close()

	ctx := context.Background()
	lease, err := pool.AcquireLease(ctx, "discord:worker-0")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	dBefore, ok := pool.Get("discord:worker-0")
	if !ok || dBefore == nil || pool.prewarmTarget(ctx, "   ") != nil || pool.Initialize(ctx) != nil {
		t.Fatalf("initial prewarm/lease state invalid")
	}
	pool.WaitBackground()
	if dAfterInit, ok := pool.Get("discord:worker-0"); !ok || dAfterInit != dBefore || dBefore.State() == StateClosed || !pool.HasDaemon("discord:worker-1") {
		t.Fatalf("expected worker-0 untouched and worker-1 pre-warmed after Initialize")
	}

	pool.MarkDirty()
	pool.WaitBackground()
	if err := pool.prewarmTarget(ctx, "discord:worker-0"); err != nil || dBefore.State() == StateClosed {
		t.Fatalf("MarkDirty or prewarmTarget while locked failed: %v", err)
	}

	_ = dBefore.Close()
	res, err := lease.Execute(ctx, &TurnContext{TurnID: "retry-2", Prompt: "retry", Ctx: ctx})
	if err != nil || res == nil || res.Response != "ok" {
		t.Fatalf("expected lease.Execute refresh on retry: res=%+v err=%v", res, err)
	}
	_ = lease.Release()

	dWorker1, _ := pool.Get("discord:worker-1")
	dWorker1.inflightMu.Lock()
	dWorker1.inflight = append(dWorker1.inflight, &TurnContext{TurnID: "inflight-1"})
	dWorker1.inflightMu.Unlock()
	pool.closeEvictedDaemon(dWorker1, "discord:worker-1")
	if dWorker1.State() == StateClosed {
		t.Fatalf("expected closeEvictedDaemon not to close daemon while inflight > 0")
	}
	dWorker1.inflightMu.Lock()
	dWorker1.inflight = nil
	cb := dWorker1.onTurnFinished
	dWorker1.inflightMu.Unlock()
	if cb == nil {
		t.Fatalf("expected onTurnFinished callback on in-flight evicted daemon")
	}
	cb(dWorker1)
	if dWorker1.State() != StateClosed {
		t.Fatalf("expected evicted daemon to close once onTurnFinished fires")
	}
	pool.closeEvictedDaemon(nil, "discord:worker-1")
}

func TestUnifiedSessionLease_ExecuteFailure_MarksDirty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		pid:    401,
		onUser: func(att int, line string) (string, string, string) { return "", "simulated failure", "ERROR" },
	})
	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	lease, err := pool.AcquireLease(ctx, "target-exec-fail")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	defer lease.Release()

	if _, execErr := lease.Execute(ctx, &TurnContext{TurnID: "t1", Prompt: "p", Ctx: ctx}); execErr == nil {
		t.Fatalf("expected Execute error, got nil")
	}
	if !lease.(*unifiedSessionLease).daemon.IsDirty() {
		t.Errorf("expected daemon to be marked dirty after execution failure")
	}
}

func TestUnifiedSessionLease_DirectMarkDirty(t *testing.T) {
	pool := NewUnifiedProcessPool(PoolConfig{}, &MockDaemonSpawner{})
	defer pool.Close()
	lease, err := pool.AcquireLease(context.Background(), "target-mark-dirty")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	defer lease.Release()
	uLease := lease.(*unifiedSessionLease)
	if uLease.daemon.IsDirty() {
		t.Fatalf("expected daemon to not be dirty initially")
	}
	lease.MarkDirty()
	if !uLease.daemon.IsDirty() {
		t.Errorf("expected daemon to be dirty after lease.MarkDirty()")
	}
	var nilLease *unifiedSessionLease
	nilLease.MarkDirty()
}

func TestUnifiedSessionLease_Release_RotationFailurePaths(t *testing.T) {
	ctx := context.Background()
	pool1 := NewUnifiedProcessPool(PoolConfig{MaxSessionTurns: 1}, &MockDaemonSpawner{})
	lease1, err := pool1.AcquireLease(ctx, "target-rot-fail")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	lease1.(*unifiedSessionLease).turnCount = 2
	pool1.Close()
	if relErr := lease1.Release(); relErr == nil {
		t.Errorf("expected Release error when RotateDaemon fails on closed pool")
	}

	cbErr := errors.New("persistence failed")
	pool2 := NewUnifiedProcessPool(PoolConfig{
		MaxSessionTurns:  1,
		OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error { return cbErr },
	}, &MockDaemonSpawner{})
	defer pool2.Close()
	lease2, err := pool2.AcquireLease(ctx, "target-cb-fail")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	lease2.(*unifiedSessionLease).turnCount = 2
	if relErr := lease2.Release(); !errors.Is(relErr, cbErr) {
		t.Errorf("expected persistence error %v, got %v", cbErr, relErr)
	}
}

func TestUnifiedProcessPool_EvictSessionAndTranscriptRescuer(t *testing.T) {
	t.Parallel()
	var nilUnified *UnifiedProcessPool
	if err := nilUnified.EvictSession("target-1"); err != nil {
		t.Errorf("expected nil error on nil UnifiedProcessPool.EvictSession")
	}
	mock := createTestSpawner()
	uPool := NewUnifiedProcessPool(PoolConfig{Model: "gemini-2.5-flash"}, mock)
	defer uPool.Close()
	if err := uPool.EvictSession("nonexistent"); err != nil {
		t.Errorf("expected nil error on nonexistent target eviction: %v", err)
	}
	sess, err := uPool.GetOrCreate(context.Background(), "target-evict-live", "")
	if err != nil || sess == nil || uPool.EvictSession("target-evict-live") != nil || uPool.HasDaemon("target-evict-live") {
		t.Fatalf("unexpected error in evict flow: sess=%v err=%v", sess, err)
	}
	var nilPool *UnifiedProcessPool
	nilPool.SetTranscriptRescuer(func(convID string, since time.Time) string { return "" })

	uPoolRescuer := NewUnifiedProcessPool(PoolConfig{Model: "gemini-2.5-flash"}, mock)
	defer uPoolRescuer.Close()
	_, _ = uPoolRescuer.GetOrCreate(context.Background(), "target-rescuer", "")
	rescuerCalled := false
	uPoolRescuer.SetTranscriptRescuer(func(convID string, since time.Time) string {
		rescuerCalled = true
		return "rescued"
	})
	if uPoolRescuer.cfg.TranscriptRescuer == nil || !rescuerCalled && uPoolRescuer.cfg.TranscriptRescuer("conv-1", time.Now()) != "rescued" || !rescuerCalled {
		t.Errorf("expected rescuer callback to be set and invoked")
	}
}

func TestIsDaemonMatch_Comprehensive(t *testing.T) {
	t.Parallel()
	dClosed, dDirty := &StreamingDaemon{state: StateClosed}, &StreamingDaemon{dirty: true, state: StateReady}
	dReady, dEmpty := &StreamingDaemon{state: StateReady, sessionID: "sess-abc"}, &StreamingDaemon{state: StateReady, sessionID: ""}
	tests := []struct {
		d    *StreamingDaemon
		sess string
		want bool
	}{
		{nil, "", false},
		{dClosed, "", false},
		{dDirty, "", false},
		{dReady, "", true},
		{dEmpty, "sess-target", true},
		{dReady, "sess-abc", true},
		{dReady, "sess-xyz", false},
	}
	for _, tc := range tests {
		if got := isDaemonMatch(tc.d, tc.sess); got != tc.want {
			t.Errorf("isDaemonMatch(%v, %q) = %v, want %v", tc.d, tc.sess, got, tc.want)
		}
	}
}

func TestAgentPool_InterfacesAndMockBehavior(t *testing.T) {
	t.Parallel()
	var _ AgentPool = (*UnifiedProcessPool)(nil)
	var _ AgentSession = (*StreamingDaemon)(nil)
	var _ AgentPool = (*mockVoicePool)(nil)
	var _ AgentSession = (*mockVoiceSession)(nil)
	var _ AgentPool = NewGeminiAPIPool(GeminiAPIPoolConfig{APIKey: "fake-key"})

	pool := newMockVoicePool("test-model")
	ctx := context.Background()
	if err := pool.Initialize(ctx); err != nil || pool.initCalled != 1 {
		t.Fatalf("unexpected init error: err=%v called=%d", err, pool.initCalled)
	}
	sess1, err := pool.GetOrCreateSession(ctx, "device-a", "")
	sess2, err2 := pool.GetOrCreateSession(ctx, "device-a", "")
	if err != nil || err2 != nil || sess1 != sess2 || sess1.SessionID() != "device-a" {
		t.Fatalf("unexpected session: sess1=%v sess2=%v err=%v", sess1, sess2, err)
	}
	var receivedPrompt string
	sess1.(*mockVoiceSession).sendFn = func(prompt string, turn *TurnContext) error {
		receivedPrompt = prompt
		return nil
	}
	if err := sess1.Send("test prompt", nil); err != nil || receivedPrompt != "test prompt" {
		t.Fatalf("unexpected Send: %v, got %q", err, receivedPrompt)
	}
	pool.getErr = errors.New("mock pool is closed")
	if err := pool.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if _, err := pool.GetOrCreateSession(ctx, "device-b", ""); err == nil {
		t.Fatal("expected error on closed mock pool")
	}
}
