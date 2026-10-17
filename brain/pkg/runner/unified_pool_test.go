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

	"github.com/azylman/aerial/brain/pkg/session"
	"github.com/google/uuid"
)

// pipeMockSpawnerOpts configures the shared pipe mock spawner helper.
type pipeMockSpawnerOpts struct {
	pid        int
	killErr    error
	initSessID string
	onSpawn    func(cfg DaemonConfig)
	echoRes    string // if set, reading from stdin responds with this SUCCESS result payload
}

// newPipeMockSpawner creates a MockDaemonSpawner backed by pipes with standard init event handling.
func newPipeMockSpawner(opts pipeMockSpawnerOpts) *MockDaemonSpawner {
	var count atomic.Int32
	return &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			idx := count.Add(1)
			if opts.onSpawn != nil {
				opts.onSpawn(cfg)
			}
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			sessID := opts.initSessID
			if sessID == "" {
				if cfg.SessionID != "" {
					sessID = cfg.SessionID
				} else {
					sessID = fmt.Sprintf("00000000-0000-0000-0000-%012d", idx)
				}
			}

			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":%q}`+"\n", sessID)))
				if opts.echoRes != "" {
					buf := make([]byte, 1024)
					n, _ := inR.Read(buf)
					if n > 0 {
						_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"result","result":{"status":"SUCCESS","response":%q}}`+"\n", opts.echoRes)))
					}
				} else {
					_, _ = io.Copy(io.Discard, inR)
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

	cfg := PoolConfig{
		PrewarmedTargets: []string{"kiosk"},
		DefaultModel:     "gemini-2.5-pro",
		MaxIdle:          24 * time.Hour,
	}

	pool := NewUnifiedProcessPool(cfg, mock)
	defer pool.Close()

	if err := pool.Initialize(context.Background()); err != nil {
		t.Fatalf("failed to initialize pool: %v", err)
	}

	d1, err1 := pool.GetOrCreate(context.Background(), "kiosk", "")
	d2, err2 := pool.GetOrCreate(context.Background(), "kiosk", "")

	if err1 != nil || err2 != nil {
		t.Fatalf("errors getting kiosk daemon: %v, %v", err1, err2)
	}
	if d1 != d2 {
		t.Errorf("expected d1 and d2 to be identical pinned instance")
	}
	if spawnCount.Load() != 1 {
		t.Errorf("expected exactly 1 spawn for kiosk, got %d", spawnCount.Load())
	}
}

func TestUnifiedProcessPool_Defaults(t *testing.T) {
	pool := NewUnifiedProcessPool(PoolConfig{}, nil)
	if pool.spawner == nil {
		t.Errorf("expected default spawner to be non-nil")
	}
	if pool.cfg.MaxIdle != 24*time.Hour {
		t.Errorf("expected MaxIdle to default to 24h, got %v", pool.cfg.MaxIdle)
	}
}

func TestUnifiedProcessPool_LifecycleBasic(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		model            string
		defaultModel     string
		checkModel       bool
		wantModel        string
		checkDaemonModel bool
		nilPool          bool
		reuseScenario    bool
		multipleTargets  bool
		spawnFail        bool
		closePool        bool
		closeErrScenario bool
		prewarmFail      bool
		prewarmedTargets []string
		wantErr          string
	}{
		{
			name:          "ReusesExistingAndReplacesClosed",
			reuseScenario: true,
		},
		{
			name:      "SpawnFailure",
			spawnFail: true,
			wantErr:   "failed to start daemon",
		},
		{
			name:            "MultipleTargets",
			multipleTargets: true,
		},
		{
			name:      "ClosedPoolRejections",
			closePool: true,
			wantErr:   "closed",
		},
		{
			name:             "CloseErrorAndIdempotent",
			closeErrScenario: true,
			wantErr:          "errors closing pool daemons",
		},
		{
			name:             "InitializePrewarmFailure",
			prewarmFail:      true,
			prewarmedTargets: []string{"bad-prewarm"},
		},
		{
			name:             "ModelBound",
			model:            "gemini-3.8-flash-low",
			checkModel:       true,
			wantModel:        "gemini-3.8-flash-low",
			checkDaemonModel: true,
		},
		{
			name:         "ModelFallback",
			defaultModel: "gemini-2.5-pro",
			checkModel:   true,
			wantModel:    "gemini-2.5-pro",
		},
		{
			name:       "NilPoolModelAndClose",
			nilPool:    true,
			checkModel: true,
			wantModel:  "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilPool {
				var nilPool *UnifiedProcessPool
				if nilPool.Model() != tc.wantModel {
					t.Errorf("expected empty string from nilPool.Model(), got %q", nilPool.Model())
				}
				if err := nilPool.Close(); err != nil {
					t.Errorf("expected nil error from nilPool.Close(), got %v", err)
				}
				return
			}

			if tc.reuseScenario {
				var spawnCount atomic.Int32
				mock := newPipeMockSpawner(pipeMockSpawnerOpts{
					initSessID: "00000000-0000-0000-0000-000000000001",
					onSpawn:    func(cfg DaemonConfig) { spawnCount.Add(1) },
				})
				pool := NewUnifiedProcessPool(PoolConfig{}, mock)
				defer pool.Close()

				d1, err := pool.GetOrCreate(context.Background(), "thread-1", "")
				if err != nil {
					t.Fatalf("first GetOrCreate failed: %v", err)
				}
				d2, err := pool.GetOrCreate(context.Background(), "thread-1", "")
				if err != nil || d1 != d2 || spawnCount.Load() != 1 {
					t.Fatalf("reuse failed: d1=%p, d2=%p, err=%v, count=%d", d1, d2, err, spawnCount.Load())
				}

				if err := d1.Close(); err != nil {
					t.Fatalf("failed closing d1: %v", err)
				}
				d3, err := pool.GetOrCreate(context.Background(), "thread-1", "")
				if err != nil || d3 == d1 || spawnCount.Load() != 2 {
					t.Fatalf("replacement failed: d1=%p, d3=%p, err=%v, count=%d", d1, d3, err, spawnCount.Load())
				}
				return
			}

			if tc.spawnFail {
				mock := &MockDaemonSpawner{
					SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
						return nil, nil, nil, nil, errors.New("simulated spawn failure")
					},
				}
				pool := NewUnifiedProcessPool(PoolConfig{}, mock)
				defer pool.Close()
				_, err := pool.GetOrCreate(context.Background(), "failed-target", "")
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("expected failed to start daemon error, got %v", err)
				}
				return
			}

			if tc.multipleTargets {
				var spawnCount atomic.Int32
				mock := newPipeMockSpawner(pipeMockSpawnerOpts{
					onSpawn: func(cfg DaemonConfig) { spawnCount.Add(1) },
				})
				pool := NewUnifiedProcessPool(PoolConfig{}, mock)
				defer pool.Close()
				d1, err1 := pool.GetOrCreate(context.Background(), "thread-alpha", "")
				d2, err2 := pool.GetOrCreate(context.Background(), "device-voice", "")
				if err1 != nil || err2 != nil || d1 == d2 || spawnCount.Load() != 2 {
					t.Errorf("multiple targets failed: d1=%p, d2=%p, count=%d", d1, d2, spawnCount.Load())
				}
				return
			}

			if tc.closePool {
				pool := NewUnifiedProcessPool(PoolConfig{}, newPipeMockSpawner(pipeMockSpawnerOpts{}))
				if err := pool.Close(); err != nil {
					t.Fatalf("close failed: %v", err)
				}
				if err := pool.Initialize(context.Background()); err == nil {
					t.Error("expected error calling Initialize on closed pool")
				}
				if _, err := pool.GetOrCreate(context.Background(), "any", ""); err == nil {
					t.Error("expected error calling GetOrCreate on closed pool")
				}
				return
			}

			if tc.closeErrScenario {
				mock := newPipeMockSpawner(pipeMockSpawnerOpts{pid: 600, killErr: errors.New("kill failed")})
				pool := NewUnifiedProcessPool(PoolConfig{}, mock)
				if _, err := pool.GetOrCreate(context.Background(), "error-target", ""); err != nil {
					t.Fatalf("failed GetOrCreate: %v", err)
				}
				err1 := pool.Close()
				if err1 == nil || !strings.Contains(err1.Error(), tc.wantErr) {
					t.Fatalf("expected close error, got %v", err1)
				}
				err2 := pool.Close()
				if err2 == nil || err2.Error() != err1.Error() {
					t.Errorf("expected idempotent close error, got %v", err2)
				}
				return
			}

			if tc.prewarmFail {
				mock := &MockDaemonSpawner{
					SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
						return nil, nil, nil, nil, errors.New("prewarm boom")
					},
				}
				pool := NewUnifiedProcessPool(PoolConfig{PrewarmedTargets: tc.prewarmedTargets}, mock)
				defer pool.Close()
				if err := pool.Initialize(context.Background()); err != nil {
					t.Fatalf("unexpected Initialize error: %v", err)
				}
				return
			}

			if tc.checkModel {
				mock := NewMockSpawner()
				pool := NewUnifiedProcessPool(PoolConfig{
					Model:        tc.model,
					DefaultModel: tc.defaultModel,
				}, mock)
				defer pool.Close()

				if pool.Model() != tc.wantModel {
					t.Fatalf("expected pool model %s, got %s", tc.wantModel, pool.Model())
				}
				if tc.checkDaemonModel {
					daemon, err := pool.GetOrCreate(context.Background(), "target-1", "")
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					if daemon == nil {
						t.Fatalf("expected non-nil daemon")
					}
					mock.mu.Lock()
					spawnCfg := mock.lastSpawnCfg
					mock.mu.Unlock()
					if spawnCfg.Model != tc.wantModel {
						t.Fatalf("expected spawned daemon model %s, got %s", tc.wantModel, spawnCfg.Model)
					}
				}
			}
		})
	}
}

func TestUnifiedProcessPool_CloseDuringSpawn(t *testing.T) {
	spawnStarted := make(chan struct{})
	allowSpawnToComplete := make(chan struct{})

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			close(spawnStarted)
			<-allowSpawnToComplete

			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000004"}` + "\n"))
			}()
			_ = inR.Close()

			return inW, outR, errR, &MockProcessHandle{pid: 500, killErr: errors.New("fail kill")}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{}, mock)

	errChan := make(chan error, 1)
	go func() {
		_, err := pool.GetOrCreate(context.Background(), "slow-target", "")
		errChan <- err
	}()

	<-spawnStarted
	_ = pool.Close()
	close(allowSpawnToComplete)

	err := <-errChan
	if err == nil || !strings.Contains(err.Error(), "process pool is closed") {
		t.Fatalf("expected process pool closed error, got %v", err)
	}
}

func TestUnifiedProcessPool_Initialize_Sequential(t *testing.T) {
	var mu sync.Mutex
	var activeSpawns, maxConcurrentSpawns int
	var order []string

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			mu.Lock()
			activeSpawns++
			if activeSpawns > maxConcurrentSpawns {
				maxConcurrentSpawns = activeSpawns
			}
			order = append(order, cfg.ThreadID)
			mu.Unlock()

			select {
			case <-ctx.Done():
				mu.Lock()
				activeSpawns--
				mu.Unlock()
				return nil, nil, nil, nil, ctx.Err()
			case <-time.After(15 * time.Millisecond):
			}

			mu.Lock()
			activeSpawns--
			mu.Unlock()

			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":"00000000-0000-0000-0000-0000000000%02d"}`+"\n", activeSpawns+10)))
				_, _ = io.ReadAll(inR)
				_ = outW.Close()
			}()

			return inW, outR, errR, &MockProcessHandle{pid: 100}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{
		PrewarmedTargets: []string{"seq-target-1", "seq-target-2", "seq-target-3"},
	}, mock)
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
	pool := NewUnifiedProcessPool(PoolConfig{
		PrewarmedTargets: []string{"cancel-target-1", "cancel-target-2"},
	}, mock)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := pool.Initialize(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
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
		{
			name:       "nil daemon",
			daemon:     nil,
			wantRotate: false,
		},
		{
			name: "turn count exceeded",
			daemon: &StreamingDaemon{
				turnCount: DefaultMaxSessionTurns,
				state:     StateReady,
			},
			wantRotate:       true,
			wantReasonSubstr: "turn count threshold exceeded",
		},
		{
			name: "raw daemon step count alone does not trigger",
			daemon: &StreamingDaemon{
				turnCount: 2,
				stepCount: 185,
				state:     StateReady,
			},
			wantRotate: false,
		},
		{
			name: "below threshold",
			daemon: &StreamingDaemon{
				turnCount: 5,
				stepCount: 100,
				state:     StateReady,
			},
			wantRotate: false,
		},
		{
			name: "inflight turn gating",
			daemon: &StreamingDaemon{
				turnCount: 15,
				stepCount: 250,
				inflight:  []*TurnContext{{TurnID: "turn-inflight"}},
				state:     StateReady,
			},
			wantRotate: false,
		},
		{
			name: "dirty daemon with 0 inflight",
			daemon: &StreamingDaemon{
				dirty: true,
				state: StateReady,
			},
			wantRotate:       true,
			wantReasonSubstr: "daemon marked dirty during in-flight turn",
		},
		{
			name: "dirty daemon with inflight > 0",
			daemon: &StreamingDaemon{
				dirty:    true,
				state:    StateExecuting,
				inflight: []*TurnContext{{TurnID: "turn-inflight"}},
			},
			wantRotate: false,
		},
		{
			name: "transcript step count threshold exceeded",
			daemon: &StreamingDaemon{
				turnCount: DefaultMinSessionTurns,
				state:     StateReady,
			},
			setupSessionMgr:  true,
			transcriptSteps:  DefaultMaxSessionSteps - 1,
			wantRotate:       true,
			wantReasonSubstr: "transcript step count threshold exceeded",
		},
		{
			name: "transcript file size threshold exceeded",
			daemon: &StreamingDaemon{
				turnCount: DefaultMinSessionTurns,
				state:     StateReady,
			},
			setupSessionMgr:  true,
			transcriptBytes:  session.DefaultMaxTranscriptBytes + 10,
			wantRotate:       true,
			wantReasonSubstr: "transcript file size threshold exceeded",
		},
		{
			name: "session DB size threshold exceeded",
			daemon: &StreamingDaemon{
				turnCount: DefaultMinSessionTurns,
				state:     StateReady,
			},
			setupSessionMgr:  true,
			sessionDBBytes:   session.DefaultMaxSessionDBBytes + 10,
			wantRotate:       true,
			wantReasonSubstr: "session DB size threshold exceeded",
		},
		{
			name: "transcript turn count threshold exceeded",
			daemon: &StreamingDaemon{
				state: StateReady,
			},
			setupSessionMgr:  true,
			transcriptTurns:  DefaultMaxSessionTurns,
			wantRotate:       true,
			wantReasonSubstr: "transcript turn count threshold exceeded",
		},
		{
			name: "under min turns standard thresholds do not rotate",
			daemon: &StreamingDaemon{
				turnCount: 1,
				state:     StateReady,
			},
			setupSessionMgr: true,
			transcriptSteps: DefaultMaxSessionSteps,
			sessionDBBytes:  DefaultMaxSessionDBBytes,
			wantRotate:      false,
		},
		{
			name: "under min turns 3x emergency step blowout rotates",
			daemon: &StreamingDaemon{
				turnCount: 1,
				state:     StateReady,
			},
			setupSessionMgr:  true,
			transcriptSteps:  DefaultEmergencyRotationMultiplier * DefaultMaxSessionSteps,
			wantRotate:       true,
			wantReasonSubstr: "transcript step count emergency threshold exceeded",
		},
		{
			name: "under min turns 3x emergency transcript size blowout rotates",
			daemon: &StreamingDaemon{
				turnCount: 1,
				state:     StateReady,
			},
			setupSessionMgr:  true,
			transcriptBytes:  DefaultEmergencyRotationMultiplier*DefaultMaxTranscriptBytes + 10,
			wantRotate:       true,
			wantReasonSubstr: "transcript file size emergency threshold exceeded",
		},
		{
			name: "under min turns 3x emergency DB size blowout rotates",
			daemon: &StreamingDaemon{
				turnCount: 2,
				state:     StateReady,
			},
			setupSessionMgr:  true,
			sessionDBBytes:   DefaultEmergencyRotationMultiplier*DefaultMaxSessionDBBytes + 10,
			wantRotate:       true,
			wantReasonSubstr: "session DB size emergency threshold exceeded",
		},
		{
			name: "under min turns dirty daemon rotates immediately",
			daemon: &StreamingDaemon{
				turnCount: 1,
				dirty:     true,
				state:     StateReady,
			},
			wantRotate:       true,
			wantReasonSubstr: "daemon marked dirty",
		},
		{
			name: "custom min turns and emergency multiplier",
			daemon: &StreamingDaemon{
				turnCount: 3,
				state:     StateReady,
			},
			setupSessionMgr: true,
			sessionDBBytes:  2*DefaultMaxSessionDBBytes + 10,
			poolCfg: PoolConfig{
				MinSessionTurns:             5,
				EmergencyRotationMultiplier: 2,
			},
			wantRotate:       true,
			wantReasonSubstr: "session DB size emergency threshold exceeded",
		},
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
					if tt.transcriptSteps > 0 {
						logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessID, ".system_generated", "logs")
						_ = os.MkdirAll(logsDir, 0755)
						_ = os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\n", tt.transcriptSteps)), 0644)
					}
					if tt.transcriptBytes > 0 {
						logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessID, ".system_generated", "logs")
						_ = os.MkdirAll(logsDir, 0755)
						_ = os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), make([]byte, tt.transcriptBytes), 0644)
					}
					if tt.sessionDBBytes > 0 {
						convDir := filepath.Join(tmpDir, "conversations")
						_ = os.MkdirAll(convDir, 0755)
						_ = os.WriteFile(filepath.Join(convDir, sessID+".db"), make([]byte, tt.sessionDBBytes), 0644)
					}
					if tt.transcriptTurns > 0 {
						logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessID, ".system_generated", "logs")
						_ = os.MkdirAll(logsDir, 0755)
						var sb strings.Builder
						for i := 0; i < tt.transcriptTurns; i++ {
							sb.WriteString(fmt.Sprintf(`{"step_index": %d, "source": "USER_EXPLICIT", "type": "USER_INPUT", "content": "Turn %d"}`+"\n", i*2, i+1))
							sb.WriteString(fmt.Sprintf(`{"step_index": %d, "source": "MODEL", "type": "PLANNER_RESPONSE", "content": "Resp %d"}`+"\n", i*2+1, i+1))
						}
						_ = os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(sb.String()), 0644)
					}
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
	tmpDir := t.TempDir()
	sm := session.New(tmpDir, tmpDir)
	pool.SetSessionManager(sm)

	pool.mu.RLock()
	gotSM := pool.cfg.SessionManager
	pool.mu.RUnlock()
	if gotSM != sm {
		t.Errorf("expected session manager to be updated")
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

	// 1. Initial creation
	d1, err := pool.GetOrCreate(ctx, "target-1", "")
	if err != nil {
		t.Fatalf("failed creating initial daemon: %v", err)
	}

	d1.mu.Lock()
	d1.turnCount = DefaultMaxSessionTurns
	d1.mu.Unlock()

	// 2. Pre-flight rotation on next GetOrCreate
	d2, err := pool.GetOrCreate(ctx, "target-1", "")
	if err != nil {
		t.Fatalf("failed GetOrCreate after threshold exceeded: %v", err)
	}
	if d2 == d1 {
		t.Fatalf("expected new daemon instance after pre-flight rotation")
	}
	if d1.State() != StateClosed {
		t.Fatalf("expected old daemon to be closed, got %v", d1.State())
	}

	// 3-6: Requesting oversized sessions preserves session ID (deferred to turn end)
	guardTests := []struct {
		name            string
		target          string
		sessID          string
		transcriptBytes int64
		sessionDBBytes  int64
		transcriptSteps int
		transcriptTurns int
	}{
		{
			name:            "transcript size exceeded",
			target:          "target-oversized",
			sessID:          "550e8400-e29b-41d4-a716-446655440001",
			transcriptBytes: session.DefaultMaxTranscriptBytes + 10,
		},
		{
			name:           "db size exceeded",
			target:         "target-oversized-db",
			sessID:         "550e8400-e29b-41d4-a716-446655440002",
			sessionDBBytes: session.DefaultMaxSessionDBBytes + 10,
		},
		{
			name:            "transcript steps exceeded",
			target:          "target-oversized-steps",
			sessID:          "550e8400-e29b-41d4-a716-446655440003",
			transcriptSteps: DefaultMaxSessionSteps,
		},
		{
			name:            "transcript turns exceeded",
			target:          "target-oversized-turns",
			sessID:          "550e8400-e29b-41d4-a716-446655440004",
			transcriptTurns: DefaultMaxSessionTurns,
		},
	}

	for _, gt := range guardTests {
		t.Run(gt.name, func(t *testing.T) {
			logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", gt.sessID, ".system_generated", "logs")
			if gt.transcriptBytes > 0 {
				_ = os.MkdirAll(logsDir, 0755)
				_ = os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), make([]byte, gt.transcriptBytes), 0644)
			}
			if gt.sessionDBBytes > 0 {
				convDir := filepath.Join(tmpDir, "conversations")
				_ = os.MkdirAll(convDir, 0755)
				_ = os.WriteFile(filepath.Join(convDir, gt.sessID+".db"), make([]byte, gt.sessionDBBytes), 0644)
			}
			if gt.transcriptSteps > 0 {
				_ = os.MkdirAll(logsDir, 0755)
				_ = os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\n", gt.transcriptSteps)), 0644)
			}
			if gt.transcriptTurns > 0 {
				_ = os.MkdirAll(logsDir, 0755)
				var sb strings.Builder
				for i := 0; i < gt.transcriptTurns; i++ {
					sb.WriteString(fmt.Sprintf(`{"step_index": %d, "source": "USER_EXPLICIT", "type": "USER_INPUT", "content": "Turn %d"}`+"\n", i*2, i+1))
					sb.WriteString(fmt.Sprintf(`{"step_index": %d, "source": "MODEL", "type": "PLANNER_RESPONSE", "content": "Resp %d"}`+"\n", i*2+1, i+1))
				}
				_ = os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(sb.String()), 0644)
			}

		d, err := pool.GetOrCreate(ctx, gt.target, gt.sessID)
		if err != nil {
			t.Fatalf("failed GetOrCreate: %v", err)
		}
		if d == nil {
			t.Fatalf("expected non-nil daemon")
		}
		lastSpawned := lastSpawnedSessionID.Load()
		if lastSpawned == nil || *lastSpawned != gt.sessID {
			t.Errorf("expected session preserved as %q, got %v", gt.sessID, lastSpawned)
		}
		})
	}
}

func TestUnifiedProcessPool_RotateDaemon(t *testing.T) {
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{})
	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	ctx := context.Background()

	// 1. Initial creation
	d1, err := pool.GetOrCreate(ctx, "target-session", "")
	if err != nil {
		t.Fatalf("failed creating initial daemon: %v", err)
	}
	d1.mu.Lock()
	d1.turnCount = DefaultMaxSessionTurns
	d1.stepCount = 185
	d1.mu.Unlock()

	// 2. Rotate existing daemon with summary compaction
	d2, err := pool.RotateDaemon(ctx, "target-session", "compacted conversation history summary")
	if err != nil {
		t.Fatalf("failed rotating daemon: %v", err)
	}
	if d2 == d1 {
		t.Errorf("expected new daemon instance after rotation")
	}
	if d1.State() != StateClosed {
		t.Errorf("expected old daemon to be closed, got %v", d1.State())
	}
	if d2.TurnCount() != 0 || d2.StepCount() != 0 {
		t.Errorf("expected fresh metrics on rotated daemon, got turns=%d, steps=%d", d2.TurnCount(), d2.StepCount())
	}

	// 3. Rotate non-existent target key
	d3, err := pool.RotateDaemon(ctx, "non-existent-target", "")
	if err != nil {
		t.Fatalf("failed rotating non-existent target: %v", err)
	}
	if d3 == nil {
		t.Fatalf("expected non-nil daemon for non-existent target rotation")
	}

	// 4. Rotate on closed pool
	if err := pool.Close(); err != nil {
		t.Fatalf("failed closing pool: %v", err)
	}
	_, errClosed := pool.RotateDaemon(ctx, "target-session", "")
	if errClosed == nil || !strings.Contains(errClosed.Error(), "process pool is closed") {
		t.Errorf("expected error on closed pool rotation, got %v", errClosed)
	}
}

func TestUnifiedProcessPool_RotateDaemon_Errors(t *testing.T) {
	var failSpawn atomic.Bool
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			if failSpawn.Load() {
				return nil, nil, nil, nil, errors.New("simulated spawn failure")
			}
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()
			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"))
			}()
			_ = inR.Close()
			return inW, outR, errR, &MockProcessHandle{pid: 100, killErr: errors.New("kill failed")}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	ctx := context.Background()

	d1, err := pool.GetOrCreate(ctx, "kill-err-target", "")
	if err != nil {
		t.Fatalf("failed creating initial daemon: %v", err)
	}
	d2, err := pool.RotateDaemon(ctx, "kill-err-target", "")
	if err != nil {
		t.Fatalf("expected RotateDaemon to succeed despite kill warning, got %v", err)
	}
	if d2 == d1 {
		t.Errorf("expected new daemon after rotation")
	}

	failSpawn.Store(true)
	_, errFail := pool.RotateDaemon(ctx, "fail-spawn-target", "")
	if errFail == nil || !strings.Contains(errFail.Error(), "failed creating rotated daemon") {
		t.Errorf("expected failed creating rotated daemon error, got %v", errFail)
	}
}

func TestUnifiedProcessPool_GetHasDaemonAndMarkDirty(t *testing.T) {
	var nilPool *UnifiedProcessPool
	if d, ok := nilPool.Get("kiosk"); ok || d != nil {
		t.Errorf("expected nil from nil UnifiedProcessPool.Get")
	}
	if nilPool.HasDaemon("kiosk") {
		t.Errorf("expected false from nil UnifiedProcessPool.HasDaemon")
	}
	nilPool.MarkDirty()

	mock := newPipeMockSpawner(pipeMockSpawnerOpts{pid: 99, initSessID: "00000000-0000-0000-0000-000000000099"})
	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	if pool.HasDaemon("kiosk") {
		t.Errorf("expected false for unspawned target")
	}

	d, err := pool.GetOrCreate(context.Background(), "kiosk", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !pool.HasDaemon("kiosk") {
		t.Errorf("expected true for spawned target")
	}
	if got, ok := pool.Get("kiosk"); !ok || got != d {
		t.Errorf("expected pool.Get to return daemon %p, got %p, ok=%v", d, got, ok)
	}

	d.SetSessionID("00000000-0000-0000-0000-000000000098")
	if d.SessionID() != "00000000-0000-0000-0000-000000000098" {
		t.Errorf("expected session ID to update to 00000000-0000-0000-0000-000000000098")
	}

	if d.IsDirty() {
		t.Errorf("expected daemon not dirty initially")
	}
	pool.MarkDirty()
	if !d.IsDirty() {
		t.Errorf("expected daemon to be dirty after pool.MarkDirty()")
	}

	_ = d.Close()
	if _, ok := pool.Get("kiosk"); ok {
		t.Errorf("expected false for closed daemon from pool.Get")
	}
}

type mockSinkCapture struct {
	toolName    string
	commandName string
	delta       string
	thinking    bool
	result      *TurnResult
	err         error
	done        chan struct{}
}

func newMockSinkCapture() *mockSinkCapture {
	return &mockSinkCapture{done: make(chan struct{}, 1)}
}

func (s *mockSinkCapture) OnTurnStarted() {}
func (s *mockSinkCapture) OnThinking()    { s.thinking = true }
func (s *mockSinkCapture) OnToolCall(tool, cmd string) {
	s.toolName = tool
	s.commandName = cmd
}
func (s *mockSinkCapture) OnToolCompleted(tool, server string, d time.Duration, status string) {}
func (s *mockSinkCapture) OnSkillActivated(skill, source string)                               {}
func (s *mockSinkCapture) OnTextDelta(delta string)                                            { s.delta += delta }
func (s *mockSinkCapture) OnResult(res *TurnResult) {
	s.result = res
	select {
	case s.done <- struct{}{}:
	default:
	}
}
func (s *mockSinkCapture) OnError(err error) {
	s.err = err
	select {
	case s.done <- struct{}{}:
	default:
	}
}

func TestStreamingDaemon_DispatchNDJSONLine_Scenarios(t *testing.T) {
	outR, outW := io.Pipe()
	inR, inW := io.Pipe()
	errR, _ := io.Pipe()

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, &MockProcessHandle{pid: 777}, nil
		},
	}

	go func() {
		_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000077"}` + "\n"))
	}()

	daemon, err := StartStreamingDaemon(context.Background(), DaemonConfig{}, mock)
	if err != nil {
		t.Fatalf("failed starting daemon: %v", err)
	}
	go func() { _, _ = io.Copy(io.Discard, inR) }()
	defer inR.Close()
	defer daemon.Close()

	sink := newMockSinkCapture()
	turnCtx := &TurnContext{
		TurnID:    "t-1",
		Prompt:    "hello",
		Sink:      sink,
		CreatedAt: time.Now(),
	}

	if err := daemon.Send("hello", turnCtx); err != nil {
		t.Fatalf("failed sending turn: %v", err)
	}

	// 1. Nested step_update with CommandLine
	stepPayload := `{"event":"step_update","step_update":{"state":"RUNNING","type":"tool_call","tool_name":"cmd_exec","tool_info":{"parameters":{"CommandLine":"git status"}}}}` + "\n"
	_, _ = outW.Write([]byte(stepPayload))

	// 2. Text delta and thinking
	deltaPayload := `{"event":"step_update","delta":"chunk1","thinking":true}` + "\n"
	_, _ = outW.Write([]byte(deltaPayload))

	// 3. Result with token usage
	resPayload := `{"event":"result","result":{"status":"SUCCESS","response":"all done","usage":{"input_tokens":10,"output_tokens":20,"thinking_tokens":5,"cache_read_tokens":2,"total_tokens":37}}}` + "\n"
	_, _ = outW.Write([]byte(resPayload))

	select {
	case <-sink.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for turn 1 result")
	}

	if sink.toolName != "cmd_exec" || sink.commandName != "git status" {
		t.Errorf("unexpected tool call capture: %s / %s", sink.toolName, sink.commandName)
	}
	if sink.delta != "chunk1" {
		t.Errorf("unexpected delta: %q", sink.delta)
	}
	if !sink.thinking {
		t.Errorf("expected thinking=true")
	}
	if sink.result == nil || sink.result.Response != "all done" {
		t.Errorf("unexpected result: %+v", sink.result)
	}
	if sink.result != nil && sink.result.Usage.TotalTokens != 37 {
		t.Errorf("unexpected total tokens: %d", sink.result.Usage.TotalTokens)
	}

	// 4. Test error result
	sinkErr := newMockSinkCapture()
	turnCtxErr := &TurnContext{
		TurnID:    "t-2",
		Prompt:    "err test",
		Sink:      sinkErr,
		CreatedAt: time.Now(),
	}
	if err := daemon.Send("err test", turnCtxErr); err != nil {
		t.Fatalf("failed sending err turn: %v", err)
	}
	errPayload := `{"event":"result","result":{"status":"ERROR","error":"custom failure message"}}` + "\n"
	_, _ = outW.Write([]byte(errPayload))

	select {
	case <-sinkErr.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for turn 2 error")
	}
	if sinkErr.err == nil || !strings.Contains(sinkErr.err.Error(), "custom failure message") {
		t.Errorf("expected custom failure message error, got %v", sinkErr.err)
	}

	// 5. Test empty error string default
	sinkErrDef := newMockSinkCapture()
	turnCtxDef := &TurnContext{
		TurnID:    "t-3",
		Prompt:    "def err test",
		Sink:      sinkErrDef,
		CreatedAt: time.Now(),
	}
	if err := daemon.Send("def err test", turnCtxDef); err != nil {
		t.Fatalf("failed sending def err turn: %v", err)
	}
	errDefPayload := `{"event":"result","result":{"status":"ERROR"}}` + "\n"
	_, _ = outW.Write([]byte(errDefPayload))

	select {
	case <-sinkErrDef.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for turn 3 error default")
	}
	if sinkErrDef.err == nil || !strings.Contains(sinkErrDef.err.Error(), "daemon execution failed") {
		t.Errorf("expected daemon execution failed error, got %v", sinkErrDef.err)
	}
}

func TestUnifiedProcessPool_TargetModelsIgnored(t *testing.T) {
	var capturedModel string
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		pid: 200,
		onSpawn: func(cfg DaemonConfig) {
			capturedModel = cfg.Model
		},
	})

	pool := NewUnifiedProcessPool(PoolConfig{
		Model: "primary-model",
		TargetModels: map[string]string{
			"ephemeral:classifier": "flash-model",
		},
	}, mock)
	defer pool.Close()

	_, err := pool.GetOrCreate(context.Background(), "ephemeral:classifier", "")
	if err != nil {
		t.Fatalf("unexpected error getting daemon: %v", err)
	}
	if capturedModel != "primary-model" {
		t.Errorf("expected model primary-model, got %q", capturedModel)
	}

	_, err = pool.GetOrCreate(context.Background(), "standard-target", "")
	if err != nil {
		t.Fatalf("unexpected error getting standard daemon: %v", err)
	}
	if capturedModel != "primary-model" {
		t.Errorf("expected model primary-model, got %q", capturedModel)
	}
}

func TestUnifiedProcessPool_ExecuteEphemeral(t *testing.T) {
	tests := []struct {
		name        string
		targetKey   string
		prompt      string
		poolCfg     PoolConfig
		spawner     DaemonSpawner
		isNilPool   bool
		isClosed    bool
		timeout     time.Duration
		preCancel   bool
		wantRes     string
		wantErr     bool
		wantErrSub  string
		wantErrIs   error
	}{
		{
			name:      "success",
			targetKey: "ephemeral:classifier",
			prompt:    "test prompt",
			poolCfg:   PoolConfig{DefaultModel: "test-model"},
			spawner:   newPipeMockSpawner(pipeMockSpawnerOpts{echoRes: "ephemeral result ok"}),
			wantRes:   "ephemeral result ok",
		},
		{
			name:      "nil pool error",
			targetKey: "key",
			prompt:    "prompt",
			isNilPool: true,
			wantErr:   true,
		},
		{
			name:       "empty targetKey error",
			targetKey:  "",
			prompt:     "prompt",
			spawner:    &MockDaemonSpawner{},
			wantErr:    true,
			wantErrSub: "target key cannot be empty",
		},
		{
			name:       "whitespace targetKey error",
			targetKey:  "   ",
			prompt:     "prompt",
			spawner:    &MockDaemonSpawner{},
			wantErr:    true,
			wantErrSub: "target key cannot be empty",
		},
		{
			name:      "context already cancelled",
			targetKey: "target",
			prompt:    "prompt",
			spawner:   newPipeMockSpawner(pipeMockSpawnerOpts{}),
			preCancel: true,
			wantErr:   true,
			wantErrIs: context.Canceled,
		},
		{
			name:      "context timeout awaiting result",
			targetKey: "target",
			prompt:    "prompt",
			spawner:   newPipeMockSpawner(pipeMockSpawnerOpts{}),
			timeout:   20 * time.Millisecond,
			wantErr:   true,
			wantErrIs: context.DeadlineExceeded,
		},
		{
			name:      "spawn failure",
			targetKey: "t1",
			prompt:    "p1",
			spawner: &MockDaemonSpawner{
				SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
					return nil, nil, nil, nil, errors.New("daemon spawn failed")
				},
			},
			wantErr:    true,
			wantErrSub: "daemon spawn failed",
		},
		{
			name:      "closed pool failure",
			targetKey: "t2",
			prompt:    "p2",
			spawner:   &MockDaemonSpawner{},
			isClosed:  true,
			wantErr:   true,
		},
		{
			name:      "stdin write failure",
			targetKey: "t3",
			prompt:    "p3",
			spawner: &MockDaemonSpawner{
				SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
					outR, outW := io.Pipe()
					inR, inW := io.Pipe()
					errR, _ := io.Pipe()
					go func() {
						defer outW.Close()
						_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000004"}` + "\n"))
					}()
					_ = inR.Close()
					_ = inW.Close()
					return inW, outR, errR, &MockProcessHandle{pid: 203}, nil
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var pool *UnifiedProcessPool
			if !tt.isNilPool {
				pool = NewUnifiedProcessPool(tt.poolCfg, tt.spawner)
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

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res != tt.wantRes {
				t.Errorf("got response %q, want %q", res, tt.wantRes)
			}
		})
	}
}


func TestUnifiedProcessPool_EphemeralLLMFunc(t *testing.T) {
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		echoRes: "llm result ok",
	})

	pool := NewUnifiedProcessPool(PoolConfig{DefaultModel: "test-model"}, mock)
	defer pool.Close()

	llmFn := pool.EphemeralLLMFunc("ephemeral:classifier")
	res, err := llmFn(context.Background(), "unused-model", "test prompt")
	if err != nil {
		t.Fatalf("unexpected error from EphemeralLLMFunc: %v", err)
	}
	if res != "llm result ok" {
		t.Errorf("expected 'llm result ok', got %q", res)
	}

	var nilPool *UnifiedProcessPool
	nilLLMFn := nilPool.EphemeralLLMFunc("key")
	if _, err := nilLLMFn(context.Background(), "model", "prompt"); err == nil {
		t.Error("expected error calling LLMFunc from nil pool")
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
		{
			name:             "CreatesGeminiDirAndSanitizesEnv",
			wantEnvSanitized: true,
		},
		{
			name:          "MkdirFailureReturnsError",
			createBlocker: true,
			wantErrSub:    "failed to create runtime home directory",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			if tc.createBlocker {
				blockerFile := filepath.Join(tmpDir, "file_blocker")
				if err := os.WriteFile(blockerFile, []byte("blocker"), 0644); err != nil {
					t.Fatalf("failed to write blocker file: %v", err)
				}

				pool := NewUnifiedProcessPool(PoolConfig{
					DefaultModel:  "test-model",
					GeminiHomeDir: filepath.Join(blockerFile, "subhome"),
				}, &MockDaemonSpawner{})
				defer pool.Close()

				_, err := pool.GetOrCreate(context.Background(), "kiosk", "")
				if err == nil {
					t.Fatal("expected error from GetOrCreate when mkdir fails, got nil")
				}
				if !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Errorf("expected error mentioning %q, got: %v", tc.wantErrSub, err)
				}
				return
			}

			var capturedEnv []string
			mock := newPipeMockSpawner(pipeMockSpawnerOpts{
				pid: 301,
				onSpawn: func(cfg DaemonConfig) {
					capturedEnv = cfg.Env
				},
			})

			poolCfg := PoolConfig{
				DefaultModel:  "test-model",
				GeminiHomeDir: tmpDir,
				Env: []string{
					"PATH=/usr/bin",
					"HOME=/root",
					"USERPROFILE=/root",
					"GEMINI_CLI_HOME=/root",
				},
			}

			pool := NewUnifiedProcessPool(poolCfg, mock)
			defer pool.Close()

			_, err := pool.GetOrCreate(context.Background(), "kiosk", "")
			if err != nil {
				t.Fatalf("unexpected error from GetOrCreate: %v", err)
			}

			geminiPath := filepath.Join(tmpDir, ".gemini")
			if fi, statErr := os.Stat(geminiPath); statErr != nil || !fi.IsDir() {
				t.Errorf("expected .gemini directory at %s, err: %v", geminiPath, statErr)
			}

			var homeCount, userProfileCount, geminiCliHomeCount int
			for _, e := range capturedEnv {
				if strings.HasPrefix(e, "HOME=") {
					homeCount++
					if e != "HOME="+tmpDir {
						t.Errorf("expected HOME=%s, got %q", tmpDir, e)
					}
				}
				if strings.HasPrefix(e, "USERPROFILE=") {
					userProfileCount++
					if e != "USERPROFILE="+tmpDir {
						t.Errorf("expected USERPROFILE=%s, got %q", tmpDir, e)
					}
				}
				if strings.HasPrefix(e, "GEMINI_CLI_HOME=") {
					geminiCliHomeCount++
					if e != "GEMINI_CLI_HOME="+tmpDir {
						t.Errorf("expected GEMINI_CLI_HOME=%s, got %q", tmpDir, e)
					}
				}
			}

			if homeCount != 1 || userProfileCount != 1 || geminiCliHomeCount != 1 {
				t.Errorf("unexpected env var counts: HOME=%d, USERPROFILE=%d, GEMINI_CLI_HOME=%d", homeCount, userProfileCount, geminiCliHomeCount)
			}
		})
	}
}

type mockReapProcessHandle struct {
	pid        int
	killCalled bool
	waitCalled bool
	killErr    error
	waitErr    error
}

func (m *mockReapProcessHandle) Pid() int {
	return m.pid
}

func (m *mockReapProcessHandle) Kill() error {
	m.killCalled = true
	return m.killErr
}

func (m *mockReapProcessHandle) Wait() error {
	m.waitCalled = true
	return m.waitErr
}

func TestStreamingDaemon_CloseReapsChildProcess(t *testing.T) {
	mockHandle := &mockReapProcessHandle{}
	daemon := &StreamingDaemon{
		handle: mockHandle,
		state:  StateReady,
	}
	if err := daemon.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if !mockHandle.waitCalled {
		t.Fatalf("expected handle.Wait() to be called on Close")
	}
}

func TestStreamingDaemon_Close_WaitErrorLogged(t *testing.T) {
	mockHandle := &mockReapProcessHandle{
		waitErr: errors.New("simulated wait failure"),
	}
	daemon := &StreamingDaemon{
		handle: mockHandle,
		state:  StateReady,
	}
	if err := daemon.Close(); err != nil {
		t.Fatalf("expected Close() to succeed despite wait warning, got %v", err)
	}
	if !mockHandle.waitCalled {
		t.Fatalf("expected handle.Wait() to be called")
	}
}

func TestUnifiedProcessPool_CloseConcurrentMultipleDaemons(t *testing.T) {
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{pid: 300})
	pool := NewUnifiedProcessPool(PoolConfig{Model: "gemini-3.8-flash-low"}, mock)
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

func TestUnifiedProcessPool_SessionRotator(t *testing.T) {
	var spawnCounter atomic.Int32
	mock := newPipeMockSpawner(pipeMockSpawnerOpts{
		onSpawn: func(cfg DaemonConfig) { spawnCounter.Add(1) },
	})

	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	ctx := context.Background()

	if should, _ := pool.ShouldRotateSession(nil); should {
		t.Error("expected ShouldRotateSession(nil) to return false")
	}
	var nilPool *UnifiedProcessPool
	if should, _ := nilPool.ShouldRotateSession(nil); should {
		t.Error("expected (*UnifiedProcessPool)(nil).ShouldRotateSession to return false")
	}

	d1, err := pool.GetOrCreate(ctx, "target-turn-hook", "")
	if err != nil {
		t.Fatalf("failed creating initial daemon: %v", err)
	}

	if should, _ := pool.ShouldRotateSession(d1); should {
		t.Error("expected clean daemon ShouldRotateSession to return false")
	}

	d1.MarkDirty()
	should, reason := pool.ShouldRotateSession(d1)
	if !should {
		t.Error("expected dirty daemon ShouldRotateSession to return true")
	}
	if !strings.Contains(reason, "dirty") {
		t.Errorf("expected reason to mention dirty, got: %s", reason)
	}

	rotatedSess, err := pool.RotateSession(ctx, "target-turn-hook")
	if err != nil {
		t.Fatalf("unexpected RotateSession error: %v", err)
	}
	if rotatedSess == nil || rotatedSess == d1 {
		t.Fatal("expected distinct non-nil rotated session")
	}
	if d1.State() != StateClosed {
		t.Errorf("expected old daemon to be closed, got state: %v", d1.State())
	}

	curDaemon, ok := pool.Get("target-turn-hook")
	if !ok || curDaemon != rotatedSess {
		t.Errorf("expected rotated session to be registered in pool as current daemon")
	}
	if spawnCounter.Load() != 2 {
		t.Errorf("expected spawn count 2, got %d", spawnCounter.Load())
	}

	if should, _ := pool.ShouldRotateSession(rotatedSess); should {
		t.Error("expected newly rotated session ShouldRotateSession to return false")
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("unexpected pool.Close() error: %v", err)
	}
	if _, err := pool.RotateSession(ctx, "target-turn-hook"); err == nil {
		t.Error("expected error from RotateSession on closed pool")
	}
	if should, _ := pool.ShouldRotateSession(rotatedSess); should {
		t.Error("expected ShouldRotateSession on closed pool to return false")
	}
}


func TestUnifiedProcessPool_DirtyEvictionAndReconciliation_Suite(t *testing.T) {
	t.Parallel()
	type testCase struct {
		name                 string
		initialKey           string
		markDirty            bool
		prewarmed            []string
		updatePre            []string
		setInflight          bool
		spawnerErrOn         int
		killErr              error
		closeBeforeMarkDirty bool
		expectEvicted        bool
		expectRewarmed       bool
		expectClosed         bool
		expectSpawns         int
	}

	tests := []testCase{
		{
			name:           "IdlePrewarmedEvictedAndReplaced",
			initialKey:     "kiosk",
			markDirty:      true,
			prewarmed:      []string{"kiosk"},
			expectEvicted:  false,
			expectRewarmed: true,
			expectClosed:   true,
			expectSpawns:   2,
		},
		{
			name:           "IdleNonPrewarmedEvictedAndClosed",
			initialKey:     "thread-nonprewarmed",
			markDirty:      true,
			prewarmed:      []string{"kiosk"},
			expectEvicted:  true,
			expectRewarmed: true,
			expectClosed:   true,
			expectSpawns:   2,
		},
		{
			name:          "InflightPreservedDuringMarkDirty",
			initialKey:    "thread-inflight",
			markDirty:     true,
			setInflight:   true,
			expectEvicted: false,
			expectClosed:  false,
			expectSpawns:  1,
		},
		{
			name:          "EvictedDaemonCloseErrorLogged",
			initialKey:    "thread-evict-err",
			markDirty:     true,
			killErr:       errors.New("simulated eviction kill error"),
			expectEvicted: true,
			expectClosed:  true,
			expectSpawns:  1,
		},
		{
			name:           "RewarmingFailureLogged",
			initialKey:     "kiosk",
			markDirty:      true,
			prewarmed:      []string{"kiosk"},
			spawnerErrOn:   2,
			expectEvicted:  true,
			expectRewarmed: false,
			expectClosed:   true,
			expectSpawns:   2,
		},
		{
			name:          "ReconcilesNewlyAddedPrewarmedTargets",
			initialKey:    "kiosk",
			markDirty:     true,
			prewarmed:     []string{"kiosk"},
			updatePre:     []string{"touch-kiosk-kitchen"},
			expectEvicted: true,
			expectClosed:  true,
			expectSpawns:  2,
		},
		{
			name:                 "ClosedPoolMarkDirtyReturnsEarly",
			closeBeforeMarkDirty: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.closeBeforeMarkDirty {
				pool := NewUnifiedProcessPool(PoolConfig{}, nil)
				if err := pool.Close(); err != nil {
					t.Fatalf("unexpected Close() error: %v", err)
				}
				pool.MarkDirty()
				return
			}

			var spawnCounter atomic.Int32
			mock := &MockDaemonSpawner{
				SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
					count := int(spawnCounter.Add(1))
					if tc.spawnerErrOn > 0 && count == tc.spawnerErrOn {
						return nil, nil, nil, nil, errors.New("simulated spawn failure")
					}
					outR, outW := io.Pipe()
					inR, inW := io.Pipe()
					errR, _ := io.Pipe()
					go func() {
						defer outW.Close()
						_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":"00000000-0000-0000-0000-%012d"}`+"\n", count)))
						_, _ = io.Copy(io.Discard, inR)
					}()
					return inW, outR, errR, &MockProcessHandle{pid: count * 100, killErr: tc.killErr}, nil
				},
			}

			pool := NewUnifiedProcessPool(PoolConfig{
				PrewarmedTargets: tc.prewarmed,
			}, mock)
			defer pool.Close()

			ctx := context.Background()
			d1, err := pool.GetOrCreate(ctx, tc.initialKey, "")
			if err != nil {
				t.Fatalf("failed initial GetOrCreate: %v", err)
			}

			if tc.setInflight {
				d1.inflight = append(d1.inflight, &TurnContext{TurnID: "t-inflight", CreatedAt: time.Now()})
			}

			if len(tc.updatePre) > 0 {
				pool.UpdatePrewarmedTargets(tc.updatePre)
			}

			if tc.markDirty {
				pool.MarkDirty()
				pool.WaitBackground()
			}

			if tc.expectClosed && d1.State() != StateClosed {
				t.Errorf("expected d1 to be closed, got state: %v", d1.State())
			}
			if !tc.expectClosed && d1.State() == StateClosed {
				t.Errorf("expected d1 not to be closed")
			}

			if tc.expectEvicted {
				if _, ok := pool.Get(tc.initialKey); ok {
					t.Errorf("expected %q to be evicted from pool", tc.initialKey)
				}
			}

			if tc.expectRewarmed {
				if dRewarmed, ok := pool.Get("kiosk"); !ok || dRewarmed == nil || dRewarmed == d1 || dRewarmed.State() == StateClosed {
					t.Errorf("expected re-warmed kiosk daemon")
				}
			}

			if len(tc.updatePre) > 0 {
				for _, target := range tc.updatePre {
					if dUpdated, ok := pool.Get(target); !ok || dUpdated == nil || dUpdated.State() == StateClosed {
						t.Errorf("expected prewarmed target %q to be active", target)
					}
				}
			}

			if tc.expectSpawns > 0 && int(spawnCounter.Load()) != tc.expectSpawns {
				t.Errorf("expected %d spawns, got %d", tc.expectSpawns, spawnCounter.Load())
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

	ctx := context.Background()
	d1, err := pool.GetOrCreate(ctx, "thread-inflight-rot", "")
	if err != nil {
		t.Fatalf("failed creating initial daemon: %v", err)
	}
	sink := newMockTurnSink()
	turn := &TurnContext{TurnID: "t-inflight", Sink: sink, CreatedAt: time.Now()}
	d1.inflight = append(d1.inflight, turn)

	pool.MarkDirty()
	if d1.State() == StateClosed {
		t.Fatalf("in-flight daemon was closed prematurely on MarkDirty")
	}
	if !d1.IsDirty() {
		t.Fatalf("expected in-flight daemon to be marked dirty")
	}

	d1.dispatchNDJSONLine(`{"event":"result","result":{"status":"SUCCESS","response":"finished"}}`)
	if should, _ := pool.ShouldRotateSession(d1); !should {
		t.Fatalf("expected dirty daemon to indicate rotation needed after turn completion")
	}
	rotatedSess, err := pool.RotateSession(ctx, "thread-inflight-rot")
	if err != nil {
		t.Fatalf("failed rotating session: %v", err)
	}
	rotatedDaemon, ok := rotatedSess.(*StreamingDaemon)
	if !ok || rotatedDaemon == nil || rotatedDaemon == d1 {
		t.Fatalf("expected new rotated daemon distinct from d1")
	}
	if d1.State() != StateClosed {
		t.Errorf("expected old daemon d1 to be closed after rotation")
	}
	if spawnCounter.Load() != 2 {
		t.Errorf("expected spawn count 2, got %d", spawnCounter.Load())
	}
}

func TestUnifiedProcessPool_MarkDirty_SingleflightBarrier(t *testing.T) {
	t.Parallel()
	var concurrentSpawns, maxConcurrentSpawns, totalSpawns atomic.Int32
	spawnGate := make(chan struct{})

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			cur := concurrentSpawns.Add(1)
			defer concurrentSpawns.Add(-1)
			for {
				oldMax := maxConcurrentSpawns.Load()
				if cur <= oldMax || maxConcurrentSpawns.CompareAndSwap(oldMax, cur) {
					break
				}
			}
			count := totalSpawns.Add(1)
			if count > 1 {
				select {
				case <-spawnGate:
				case <-ctx.Done():
					return nil, nil, nil, nil, ctx.Err()
				}
			}
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()
			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":"00000000-0000-0000-0000-%012d"}`+"\n", count)))
				_, _ = io.Copy(io.Discard, inR)
			}()
			return inW, outR, errR, &MockProcessHandle{pid: int(count * 100)}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{
		PrewarmedTargets: []string{"kiosk"},
	}, mock)
	defer pool.Close()

	ctx := context.Background()
	d1, err := pool.GetOrCreate(ctx, "kiosk", "")
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
	if maxConcurrentSpawns.Load() > 1 {
		t.Errorf("expected at most 1 concurrent spawn during pre-warming, got %d", maxConcurrentSpawns.Load())
	}
	if totalSpawns.Load() != 2 {
		t.Errorf("expected exactly 2 total spawns, got %d", totalSpawns.Load())
	}
}

func TestUnifiedProcessPool_GetOrCreate_RejectsDirtyDaemon(t *testing.T) {
	type testCase struct {
		name         string
		killErr      error
		expectSpawns int
	}

	tests := []testCase{
		{
			name:         "SpawnsFreshAndClosesDirty",
			expectSpawns: 2,
		},
		{
			name:         "DirtyDaemonCloseErrorLogged",
			killErr:      errors.New("simulated dirty daemon kill error"),
			expectSpawns: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var spawnCounter atomic.Int32
			mock := &MockDaemonSpawner{
				SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
					count := int(spawnCounter.Add(1))
					outR, outW := io.Pipe()
					inR, inW := io.Pipe()
					errR, _ := io.Pipe()
					go func() {
						defer outW.Close()
						_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":"00000000-0000-0000-0000-%012d"}`+"\n", count)))
						_, _ = io.Copy(io.Discard, inR)
					}()
					return inW, outR, errR, &MockProcessHandle{pid: count * 100, killErr: tc.killErr}, nil
				},
			}

			pool := NewUnifiedProcessPool(PoolConfig{}, mock)
			defer pool.Close()

			ctx := context.Background()
			d1, err := pool.GetOrCreate(ctx, "thread-dirty-target", "")
			if err != nil {
				t.Fatalf("initial GetOrCreate failed: %v", err)
			}

			d1.MarkDirty()
			if !d1.IsDirty() {
				t.Fatalf("expected d1 to be marked dirty")
			}

			d2, err := pool.GetOrCreate(ctx, "thread-dirty-target", "")
			if err != nil {
				t.Fatalf("second GetOrCreate failed: %v", err)
			}
			if d2 == d1 {
				t.Errorf("expected GetOrCreate to reject dirty daemon and return a fresh daemon")
			}
			if d2.IsDirty() {
				t.Errorf("expected fresh daemon d2 not to be dirty")
			}

			pool.WaitBackground()
			if d1.State() != StateClosed {
				t.Errorf("expected dirty daemon d1 to be closed, got state: %v", d1.State())
			}
			if tc.expectSpawns > 0 && int(spawnCounter.Load()) != tc.expectSpawns {
				t.Errorf("expected exactly %d spawns, got %d", tc.expectSpawns, spawnCounter.Load())
			}
		})
	}
}

func TestUnifiedProcessPool_PrewarmedTargets_DynamicUpdate(t *testing.T) {
	type testCase struct {
		name          string
		nilPool       bool
		initial       []string
		update        []string
		mutateFirst   bool
		expectedSlice []string
	}

	tests := []testCase{
		{
			name:    "NilPool_ReturnsNil",
			nilPool: true,
			update:  []string{"test"},
		},
		{
			name:          "InitialAndDefensiveCopy",
			initial:       []string{"a", "b"},
			mutateFirst:   true,
			expectedSlice: []string{"a", "b"},
		},
		{
			name:          "UpdateFiltersWhitespaceAndEmpty",
			update:        []string{"  target1  ", "", "   ", "target2"},
			expectedSlice: []string{"target1", "target2"},
		},
		{
			name:          "UpdateDeduplicatesTargets",
			update:        []string{"kiosk", "kiosk", "touch-kiosk-kitchen", "  kiosk  "},
			expectedSlice: []string{"kiosk", "touch-kiosk-kitchen"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.nilPool {
				var p *UnifiedProcessPool
				if got := p.PrewarmedTargets(); got != nil {
					t.Errorf("expected nil from nil pool, got %v", got)
				}
				p.UpdatePrewarmedTargets(tc.update)
				return
			}

			pool := NewUnifiedProcessPool(PoolConfig{
				PrewarmedTargets: tc.initial,
			}, &MockDaemonSpawner{})
			defer pool.Close()

			if tc.mutateFirst {
				targets := pool.PrewarmedTargets()
				if len(targets) != len(tc.expectedSlice) {
					t.Fatalf("unexpected targets length: %v", targets)
				}
				targets[0] = "mutated"
				fresh := pool.PrewarmedTargets()
				if fresh[0] != tc.expectedSlice[0] {
					t.Errorf("defensive copy violated: pool target mutated to %q", fresh[0])
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
					t.Errorf("expected nil after setting nil targets, got %v", got)
				}
			}
		})
	}
}

func TestUnifiedProcessPool_SessionPropagation_Suite(t *testing.T) {
	type testCase struct {
		name             string
		callSession      bool
		inputSessID      string
		secondSessID     string
		initialTurns     int
		expectCapturedSess string
		expectSessID     string
		expectSecondDiff bool
		expectSpawns     int
	}

	tests := []testCase{
		{
			name:               "PropagatesCustomSessionID",
			inputSessID:        "11111111-2222-3333-4444-555555555555",
			secondSessID:       "11111111-2222-3333-4444-555555555555",
			expectCapturedSess: "11111111-2222-3333-4444-555555555555",
			expectSessID:       "11111111-2222-3333-4444-555555555555",
			expectSpawns:       1,
		},
		{
			name:               "FreshSessionOnEmptyID",
			inputSessID:        "",
			expectCapturedSess: "",
			expectSessID:       "00000000-0000-0000-0000-000000000001",
			expectSpawns:       1,
		},
		{
			name:               "MismatchedSessionIDRotates",
			inputSessID:        "11111111-1111-1111-1111-111111111111",
			secondSessID:       "22222222-2222-2222-2222-222222222222",
			expectCapturedSess: "11111111-1111-1111-1111-111111111111",
			expectSessID:       "11111111-1111-1111-1111-111111111111",
			expectSecondDiff:   true,
			expectSpawns:       2,
		},
		{
			name:               "GetOrCreateSession_PropagatesSessionID",
			callSession:        true,
			inputSessID:        "33333333-3333-3333-3333-333333333333",
			expectCapturedSess: "33333333-3333-3333-3333-333333333333",
			expectSessID:       "33333333-3333-3333-3333-333333333333",
			expectSpawns:       1,
		},
		{
			name:               "PropagatesInitialTurnCount",
			inputSessID:        "44444444-4444-4444-4444-444444444444",
			initialTurns:       5,
			expectCapturedSess: "44444444-4444-4444-4444-444444444444",
			expectSessID:       "44444444-4444-4444-4444-444444444444",
			expectSpawns:       1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedCfg DaemonConfig
			var spawnCount atomic.Int32

			mock := &MockDaemonSpawner{
				SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
					count := spawnCount.Add(1)
					capturedCfg = cfg
					outR, outW := io.Pipe()
					inR, inW := io.Pipe()
					errR, _ := io.Pipe()

					go func() {
						defer outW.Close()
						sessID := cfg.SessionID
						if sessID == "" {
							sessID = fmt.Sprintf("00000000-0000-0000-0000-%012d", count)
						}
						_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":%q}`+"\n", sessID)))
						_, _ = io.Copy(io.Discard, inR)
					}()

					return inW, outR, errR, &MockProcessHandle{pid: int(count * 100)}, nil
				},
			}

			pool := NewUnifiedProcessPool(PoolConfig{}, mock)
			defer pool.Close()

			ctx := context.Background()

			if tc.callSession {
				sess, err := pool.GetOrCreateSession(ctx, "target-prop-suite", tc.inputSessID)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if sess.SessionID() != tc.expectSessID {
					t.Errorf("expected session ID %q, got %q", tc.expectSessID, sess.SessionID())
				}
				return
			}

			var d1 *StreamingDaemon
			var err error
			if tc.initialTurns > 0 {
				d1, err = pool.GetOrCreate(ctx, "target-prop-suite", tc.inputSessID, tc.initialTurns)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if capturedCfg.InitialTurnCount != tc.initialTurns {
					t.Errorf("expected InitialTurnCount %d, got %d", tc.initialTurns, capturedCfg.InitialTurnCount)
				}
				if d1.TurnCount() != tc.initialTurns {
					t.Errorf("expected daemon TurnCount %d, got %d", tc.initialTurns, d1.TurnCount())
				}
			} else {
				d1, err = pool.GetOrCreate(ctx, "target-prop-suite", tc.inputSessID)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			if capturedCfg.SessionID != tc.expectCapturedSess {
				t.Errorf("expected captured SessionID %q, got %q", tc.expectCapturedSess, capturedCfg.SessionID)
			}
			if d1.SessionID() != tc.expectSessID {
				t.Errorf("expected d1 SessionID %q, got %q", tc.expectSessID, d1.SessionID())
			}

			if tc.secondSessID != "" {
				d2, err2 := pool.GetOrCreate(ctx, "target-prop-suite", tc.secondSessID)
				if err2 != nil {
					t.Fatalf("second GetOrCreate error: %v", err2)
				}
				if tc.expectSecondDiff {
					if d2 == d1 {
						t.Errorf("expected new daemon instance after session mismatch")
					}
					if d2.SessionID() != tc.secondSessID {
						t.Errorf("expected d2 session %q, got %q", tc.secondSessID, d2.SessionID())
					}
					pool.WaitBackground()
					if d1.State() != StateClosed {
						t.Errorf("expected old daemon d1 to be closed after session mismatch, got %v", d1.State())
					}
				} else {
					if d2 != d1 {
						t.Errorf("expected same daemon instance returned for identical session ID")
					}
				}
			}

			if tc.expectSpawns > 0 && int(spawnCount.Load()) != tc.expectSpawns {
				t.Errorf("expected exactly %d spawns, got %d", tc.expectSpawns, spawnCount.Load())
			}
		})
	}
}


func TestUnifiedProcessPool_AcquireLease_Suite(t *testing.T) {
	type leaseTestCase struct {
		name                 string
		validationNilPool    bool
		validationEmptyTarget bool
		existingTurnCount    int
		customExistingSessID string
		maxTurns             int
		targetLockTimeout    time.Duration
		onRotatedErr         error
		respPayload          string
		respErr              string
		respStatus           string
		twoAttemptsThrottle  bool
		concurrencyConflict  bool
		expectAcquireErr     bool
		expectExecErr        bool
		expectReleaseErr     bool
		expectRotated        bool
		expectColdBefore     bool
		expectColdAfter      bool
		expectPrevSess       string
		expectTurnCount      int
		expectSubstantive    string
		expectPreservedSess  bool
	}

	tests := []leaseTestCase{
		{
			name:                 "BasicExecution",
			customExistingSessID: "550e8400-e29b-41d4-a716-446655440001",
			expectPrevSess:       "550e8400-e29b-41d4-a716-446655440000",
			respPayload:          "lease reply",
			expectColdBefore:     true,
			expectColdAfter:      false,
			expectTurnCount:      1,
			expectSubstantive:    "lease reply",
		},
		{
			name:        "RotationAndPersistence",
			maxTurns:    1,
			respPayload: "ok",
			expectRotated: true,
		},
		{
			name:             "PersistenceFailureRollback",
			maxTurns:         1,
			respPayload:      "ok",
			onRotatedErr:     errors.New("simulated DB latch timeout"),
			expectReleaseErr: true,
		},
		{
			name:                "ConcurrencySerialization",
			targetLockTimeout:   200 * time.Millisecond,
			respPayload:         "ok",
			concurrencyConflict: true,
		},
		{
			name:          "CancelRotationOnQuotaExhausted",
			maxTurns:      1,
			respErr:       "RESOURCE_EXHAUSTED: quota exceeded. Resets in 30s.",
			respStatus:    "ERROR",
			expectExecErr: true,
			expectRotated: false,
		},
		{
			name:              "RotatesWhenResponseSubstantiveDespiteStderrThrottle",
			maxTurns:          1,
			respPayload:       "Task completed successfully and verified.",
			respErr:           "API error (attempt 1): RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s.",
			respStatus:        "DONE",
			expectRotated:     true,
			expectSubstantive: "Task completed successfully and verified.",
		},
		{
			name:                "ClearsCancelRotationOnSuccessfulRetry",
			maxTurns:            1,
			twoAttemptsThrottle: true,
			expectRotated:       true,
			expectSubstantive:   "Retry succeeded with substantive output.",
		},
		{
			name:              "ConversationalRateLimitTextDoesNotCancelRotation",
			maxTurns:          1,
			respPayload:       "I hit an external rate limit reached earlier, but I cached the result.",
			respStatus:        "DONE",
			expectRotated:     true,
			expectSubstantive: "rate limit reached",
		},
		{
			name:              "Validation_NilPool",
			validationNilPool: true,
			expectAcquireErr:  true,
		},
		{
			name:                  "Validation_EmptyTarget",
			validationEmptyTarget: true,
			expectAcquireErr:      true,
		},
		{
			name:              "PropagatesInitialTurnCountAndRotates",
			existingTurnCount: DefaultMaxSessionTurns - 1,
			respPayload:       "ok",
			expectRotated:     true,
			expectTurnCount:   DefaultMaxSessionTurns,
		},
		{
			name:                "DefersRotationToTurnEndEvenIfExceeded",
			existingTurnCount:   DefaultMaxSessionTurns,
			respPayload:         "ok",
			expectRotated:       true,
			expectPreservedSess: true,
		},
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
			var attemptCount int

			pinnedSessID := tc.customExistingSessID
			if pinnedSessID == "" {
				pinnedSessID = uuid.New().String()
			}

			mock := &MockDaemonSpawner{
				SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
					inR, inW := io.Pipe()
					outR, outW := io.Pipe()
					errR, _ := io.Pipe()

					go func() {
						sessID := cfg.SessionID
						if sessID == "" {
							sessID = pinnedSessID
						}
						_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":%q}`+"\n", sessID)))

						buf := make([]byte, 4096)
						for {
							n, err := inR.Read(buf)
							if err != nil {
								break
							}
							if strings.Contains(string(buf[:n]), `"event":"user"`) {
								attemptCount++
								if tc.twoAttemptsThrottle {
									if attemptCount == 1 {
										_, _ = outW.Write([]byte(`{"event":"result","response":"","error":"RESOURCE_EXHAUSTED: quota exceeded. Resets in 5s.","status":"ERROR"}` + "\n"))
									} else {
										_, _ = outW.Write([]byte(`{"event":"result","response":"Retry succeeded with substantive output.","status":"DONE"}` + "\n"))
									}
									continue
								}

								status := tc.respStatus
								if status == "" {
									if tc.respErr != "" && tc.respPayload == "" {
										status = "ERROR"
									} else {
										status = "SUCCESS"
									}
								}
								line := fmt.Sprintf(`{"event":"result","response":%q,"error":%q,"status":%q}`+"\n", tc.respPayload, tc.respErr, status)
								_, _ = outW.Write([]byte(line))
							}
						}
					}()

					return inW, outR, errR, &MockProcessHandle{pid: 350}, nil
				},
			}

			poolCfg := PoolConfig{
				MaxSessionTurns:   tc.maxTurns,
				TargetLockTimeout: tc.targetLockTimeout,
				GetSessionRecord: func(ctx context.Context, targetKey string) (SessionRecord, error) {
					return SessionRecord{
						ActiveSessionID:   pinnedSessID,
						PreviousSessionID: tc.expectPrevSess,
						TurnCount:         tc.existingTurnCount,
					}, nil
				},
				OnSessionRotated: func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
					rotationCalled = true
					rotatedOld = oldSessionID
					rotatedNew = newSessionID
					return tc.onRotatedErr
				},
			}

			pool := NewUnifiedProcessPool(poolCfg, mock)
			defer pool.Close()

			targetKey := "target-" + strings.ToLower(tc.name)
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
				if _, err2 := pool.AcquireLease(ctxTimeout, targetKey); err2 == nil {
					t.Fatalf("expected concurrent AcquireLease to time out while lease is held")
				}
			}

			turnCtx := &TurnContext{
				TurnID: "turn-1",
				Prompt: "prompt-1",
				Ctx:    ctx,
			}

			res, execErr := lease.Execute(ctx, turnCtx)
			if tc.twoAttemptsThrottle {
				if execErr == nil {
					t.Fatal("expected attempt 1 to fail on throttle")
				}
				turnCtx2 := &TurnContext{TurnID: "turn-1-retry", Prompt: "attempt 2", Ctx: ctx}
				res2, execErr2 := lease.Execute(ctx, turnCtx2)
				if execErr2 != nil {
					t.Fatalf("attempt 2 failed: %v", execErr2)
				}
				res = res2
				execErr = nil
			}

			if tc.expectExecErr {
				if execErr == nil {
					t.Fatalf("expected Execute error, got nil")
				}
			} else if execErr != nil {
				t.Fatalf("unexpected Execute error: %v", execErr)
			}

			if tc.expectSubstantive != "" {
				if res == nil || !strings.Contains(res.Response, tc.expectSubstantive) {
					t.Errorf("expected response containing %q, got: %+v", tc.expectSubstantive, res)
				}
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
					t.Errorf("expected daemon to be evicted from pool after failure")
				}
				return
			}
			if relErr != nil {
				t.Fatalf("unexpected Release error: %v", relErr)
			}

			if tc.concurrencyConflict {
				lease3, err3 := pool.AcquireLease(ctx, targetKey)
				if err3 != nil {
					t.Fatalf("lease3 AcquireLease error after lease1 release: %v", err3)
				}
				_ = lease3.Release()
			}

			if tc.expectRotated {
				if !rotationCalled {
					t.Errorf("expected rotation to be called")
				}
				if rotatedOld != pinnedSessID {
					t.Errorf("expected rotatedOld %q, got %q", pinnedSessID, rotatedOld)
				}
				if rotatedNew == "" || rotatedNew == rotatedOld {
					t.Errorf("expected fresh rotatedNew, got %q", rotatedNew)
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
	wrapper := &leaseTurnSinkWrapper{
		inner: inner,
		buf:   buf,
	}

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

	if !inner.started {
		t.Error("expected inner.started to be true")
	}
	if inner.thinking != 1 {
		t.Errorf("expected inner.thinking == 1, got %d", inner.thinking)
	}

	// Nil inner sink safety
	nilWrapper := &leaseTurnSinkWrapper{
		inner: nil,
		buf:   NewBufferingTurnSink(BufferingTurnSinkConfig{}),
	}
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
	mock := &MockDaemonSpawner{}
	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	getRec := func(ctx context.Context, targetKey string) (SessionRecord, error) {
		return SessionRecord{}, nil
	}
	onRot := func(ctx context.Context, targetKey, oldSessionID, newSessionID string) error {
		return nil
	}

	pool.SetSessionCallbacks(getRec, onRot)
	if pool.cfg.GetSessionRecord == nil || pool.cfg.OnSessionRotated == nil {
		t.Error("expected callbacks to be set")
	}

	// Nil receiver safety
	var nilPool *UnifiedProcessPool
	nilPool.SetSessionCallbacks(getRec, onRot)
}

func TestUnifiedProcessPool_VanadisChunkIDEnvAndRefresh(t *testing.T) {
	t.Parallel()

	if got := VanadisChunkIDFromContext(nil); got != "" {
		t.Fatalf("expected empty chunk ID from nil context, got %q", got)
	}
	if got := VanadisChunkIDFromContext(WithVanadisChunkID(nil, "chunk-from-nil\r\n")); got != "chunk-from-nil" {
		t.Fatalf("expected chunk-from-nil, got %q", got)
	}
	if got := VanadisChunkIDFromContext(WithVanadisChunkID(context.Background(), "   \r\n ")); got != "" {
		t.Fatalf("expected empty chunk ID for whitespace input, got %q", got)
	}
	var nilDaemon *StreamingDaemon
	if got := nilDaemon.VanadisChunkID(); got != "" {
		t.Fatalf("expected empty chunk ID from nil daemon, got %q", got)
	}
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

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			mu.Lock()
			spawnedEnvs = append(spawnedEnvs, append([]string(nil), cfg.Env...))
			spawnedSessions = append(spawnedSessions, cfg.SessionID)
			pid := 510 + len(spawnedEnvs)
			mu.Unlock()

			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, _ := io.Pipe()
			go func() {
				sessID := cfg.SessionID
				if sessID == "" {
					sessID = uuid.New().String()
				}
				_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":%q}`+"\n", sessID)))
				buf := make([]byte, 4096)
				for {
					n, err := inR.Read(buf)
					if err != nil {
						return
					}
					if strings.Contains(string(buf[:n]), `"event":"user"`) {
						_, _ = outW.Write([]byte(`{"event":"result","response":"ok"}` + "\n"))
					}
				}
			}()
			return inW, outR, errR, &MockProcessHandle{pid: pid}, nil
		},
	}

	pinnedSessID := uuid.New().String()
	pool := NewUnifiedProcessPool(PoolConfig{
		GeminiHomeDir: t.TempDir(),
		Env:           []string{"AERIAL_VANADIS_CHUNK_ID=stale-parent", "BASE_VAR=1"},
		GetSessionRecord: func(ctx context.Context, targetKey string) (SessionRecord, error) {
			return SessionRecord{
				ActiveSessionID:   pinnedSessID,
				PreviousSessionID: "prev-sess-1",
				TurnCount:         0,
			}, nil
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

	ctxChunk1 := WithVanadisChunkID(ctx, "chunk-1")
	lease1, err := pool.AcquireLease(ctxChunk1, "thread-vanadis")
	if err != nil || lease1.SessionID() != pinnedSessID {
		t.Fatalf("unexpected lease1: err=%v sess=%q", err, lease1.SessionID())
	}
	if _, err := lease1.Execute(ctxChunk1, &TurnContext{TurnID: "t1", Prompt: "chunk 1", VanadisChunkID: "chunk-1", Ctx: ctxChunk1}); err != nil {
		t.Fatalf("Execute 1 error: %v", err)
	}
	_ = lease1.Release()

	lease2, err := pool.AcquireLease(ctx, "thread-vanadis")
	if err != nil {
		t.Fatalf("AcquireLease 2 error: %v", err)
	}
	if _, err := lease2.Execute(ctx, &TurnContext{TurnID: "t2", Prompt: "chunk 2", VanadisChunkID: "chunk-2", Ctx: ctx}); err != nil {
		t.Fatalf("Execute 2 error: %v", err)
	}
	_ = lease2.Release()

	d3, err := pool.GetOrCreate(ctx, "thread-vanadis", "")
	if err != nil || d3.SessionID() != pinnedSessID || d3.VanadisChunkID() != "" {
		t.Fatalf("GetOrCreate 3 failed: err=%v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(spawnedEnvs) != 5 {
		t.Fatalf("expected 5 spawns, got %d", len(spawnedEnvs))
	}
	for i, s := range spawnedSessions {
		if s != pinnedSessID {
			t.Errorf("spawn %d expected SessionID %q, got %q", i, pinnedSessID, s)
		}
	}
}

func TestUnifiedProcessPool_PrewarmAndLeaseResilience(t *testing.T) {
	var seq atomic.Int32
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			id := int(seq.Add(1))
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, _ := io.Pipe()
			go func() {
				sessID := cfg.SessionID
				if sessID == "" {
					sessID = fmt.Sprintf("00000000-0000-0000-0000-%012d", id)
				}
				_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":%q}`+"\n", sessID)))
				buf := make([]byte, 4096)
				for {
					n, err := inR.Read(buf)
					if err != nil {
						return
					}
					if strings.Contains(string(buf[:n]), `"event":"user"`) {
						_, _ = outW.Write([]byte(`{"event":"result","response":"ok"}` + "\n"))
					}
				}
			}()
			return inW, outR, errR, &MockProcessHandle{pid: 900 + id}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{
		GeminiHomeDir:    t.TempDir(),
		PrewarmedTargets: []string{"discord:worker-0", "discord:worker-1"},
	}, mock)
	defer pool.Close()

	ctx := context.Background()
	chunkCtx := WithVanadisChunkID(ctx, "chunk-recovery-1")
	lease, err := pool.AcquireLease(chunkCtx, "discord:worker-0")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	dBefore, ok := pool.Get("discord:worker-0")
	if !ok || dBefore == nil || dBefore.VanadisChunkID() != "chunk-recovery-1" {
		t.Fatalf("expected active chunk-recovery-1 daemon on discord:worker-0")
	}
	if err := pool.prewarmTarget(ctx, "   "); err != nil {
		t.Fatalf("expected nil for empty targetKey, got %v", err)
	}
	if err := pool.Initialize(ctx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	pool.WaitBackground()
	if dAfterInit, ok := pool.Get("discord:worker-0"); !ok || dAfterInit != dBefore || dBefore.State() == StateClosed || !pool.HasDaemon("discord:worker-1") {
		t.Fatalf("expected worker-0 untouched and worker-1 pre-warmed after Initialize")
	}

	pool.MarkDirty()
	pool.WaitBackground()
	if err := pool.prewarmTarget(ctx, "discord:worker-0"); err != nil || dBefore.State() == StateClosed {
		t.Fatalf("MarkDirty or prewarmTarget while locked failed or closed daemon: %v", err)
	}

	_ = dBefore.Close()
	res, err := lease.Execute(chunkCtx, &TurnContext{TurnID: "retry-2", Prompt: "retry", VanadisChunkID: "chunk-recovery-1", Ctx: chunkCtx})
	if err != nil || res == nil || res.Response != "ok" {
		t.Fatalf("expected lease.Execute to refresh closed daemon on retry: res=%+v err=%v", res, err)
	}
	_ = lease.Release()

	dWorker1, _ := pool.Get("discord:worker-1")
	dWorker1.inflightMu.Lock()
	dWorker1.inflight = append(dWorker1.inflight, &TurnContext{TurnID: "inflight-1"})
	dWorker1.inflightMu.Unlock()
	pool.closeEvictedDaemon(dWorker1, "discord:worker-1")
	if dWorker1.State() == StateClosed {
		t.Fatalf("expected closeEvictedDaemon not to close daemon while InflightCount > 0")
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
