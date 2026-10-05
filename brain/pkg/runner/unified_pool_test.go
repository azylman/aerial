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
)

func TestUnifiedProcessPool_SingleflightPrewarming(t *testing.T) {
	var spawnCount atomic.Int32

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			spawnCount.Add(1)
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000010"}` + "\n"))
				_, _ = io.ReadAll(inR)
				_ = outW.Close()
			}()

			return inW, outR, errR, &MockProcessHandle{pid: 100}, nil
		},
	}

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

	// Concurrent callers requesting "kiosk" should join the same daemon, not re-spawn
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

func TestUnifiedProcessPool_GetOrCreate_ReusesExistingAndReplacesClosed(t *testing.T) {
	var spawnCount atomic.Int32

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			spawnCount.Add(1)
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"))
				_, _ = io.Copy(io.Discard, inR)
			}()

			return inW, outR, errR, &MockProcessHandle{pid: 200}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	d1, err := pool.GetOrCreate(context.Background(), "thread-1", "")
	if err != nil {
		t.Fatalf("first GetOrCreate failed: %v", err)
	}

	// Reusing live daemon
	d2, err := pool.GetOrCreate(context.Background(), "thread-1", "")
	if err != nil {
		t.Fatalf("second GetOrCreate failed: %v", err)
	}
	if d1 != d2 {
		t.Errorf("expected d1 and d2 to be identical instance")
	}
	if spawnCount.Load() != 1 {
		t.Errorf("expected spawnCount 1, got %d", spawnCount.Load())
	}

	// Close daemon to simulate termination
	if err := d1.Close(); err != nil {
		t.Fatalf("failed closing d1: %v", err)
	}

	// Calling GetOrCreate on closed daemon should spawn a replacement
	d3, err := pool.GetOrCreate(context.Background(), "thread-1", "")
	if err != nil {
		t.Fatalf("third GetOrCreate failed: %v", err)
	}
	if d3 == d1 {
		t.Errorf("expected new daemon instance after replacement")
	}
	if spawnCount.Load() != 2 {
		t.Errorf("expected spawnCount 2, got %d", spawnCount.Load())
	}
}

func TestUnifiedProcessPool_GetOrCreate_SpawnFailure(t *testing.T) {
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return nil, nil, nil, nil, errors.New("simulated spawn failure")
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	d, err := pool.GetOrCreate(context.Background(), "failed-target", "")
	if err == nil {
		t.Fatalf("expected error from failed spawn, got nil daemon: %v", d)
	}
	if !strings.Contains(err.Error(), "failed to start daemon") {
		t.Errorf("unexpected error format: %v", err)
	}
}

func TestUnifiedProcessPool_MultipleTargets(t *testing.T) {
	var spawnCount atomic.Int32

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			spawnCount.Add(1)
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000002"}` + "\n"))
			}()
			_ = inR.Close()

			return inW, outR, errR, &MockProcessHandle{pid: 300}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	d1, err1 := pool.GetOrCreate(context.Background(), "thread-alpha", "")
	d2, err2 := pool.GetOrCreate(context.Background(), "device-voice", "")

	if err1 != nil || err2 != nil {
		t.Fatalf("failed getting targets: %v, %v", err1, err2)
	}
	if d1 == d2 {
		t.Errorf("expected different instances for distinct target keys")
	}
	if spawnCount.Load() != 2 {
		t.Errorf("expected exactly 2 spawns, got %d", spawnCount.Load())
	}
}

func TestUnifiedProcessPool_ClosedPoolRejections(t *testing.T) {
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000003"}` + "\n"))
			}()
			_ = inR.Close()

			return inW, outR, errR, &MockProcessHandle{pid: 400}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	if err := pool.Close(); err != nil {
		t.Fatalf("failed closing pool: %v", err)
	}

	if err := pool.Initialize(context.Background()); err == nil {
		t.Error("expected error calling Initialize on closed pool")
	}

	if _, err := pool.GetOrCreate(context.Background(), "any", ""); err == nil {
		t.Error("expected error calling GetOrCreate on closed pool")
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
	// Close pool while spawn is in-flight
	_ = pool.Close()
	close(allowSpawnToComplete)

	err := <-errChan
	if err == nil {
		t.Fatal("expected error from GetOrCreate when pool was closed during spawn")
	}
	if !strings.Contains(err.Error(), "process pool is closed") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestUnifiedProcessPool_CloseErrorAndIdempotent(t *testing.T) {
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000005"}` + "\n"))
			}()
			_ = inR.Close()

			return inW, outR, errR, &MockProcessHandle{pid: 600, killErr: errors.New("kill failed")}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	_, err := pool.GetOrCreate(context.Background(), "error-target", "")
	if err != nil {
		t.Fatalf("failed GetOrCreate: %v", err)
	}

	errClose := pool.Close()
	if errClose == nil {
		t.Fatal("expected error on pool.Close() due to kill failure")
	}
	if !strings.Contains(errClose.Error(), "errors closing pool daemons") {
		t.Errorf("unexpected close error: %v", errClose)
	}

	// Second Close() should return the same error (idempotent)
	errClose2 := pool.Close()
	if errClose2 == nil || errClose2.Error() != errClose.Error() {
		t.Errorf("expected idempotent close error, got %v", errClose2)
	}
}

func TestUnifiedProcessPool_InitializePrewarmFailure(t *testing.T) {
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return nil, nil, nil, nil, errors.New("prewarm boom")
		},
	}

	cfg := PoolConfig{
		PrewarmedTargets: []string{"bad-prewarm"},
	}
	pool := NewUnifiedProcessPool(cfg, mock)
	defer pool.Close()

	if err := pool.Initialize(context.Background()); err != nil {
		t.Fatalf("unexpected Initialize error: %v", err)
	}
}

func TestUnifiedProcessPool_Initialize_Sequential(t *testing.T) {
	var mu sync.Mutex
	var activeSpawns int
	maxConcurrentSpawns := 0
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

			// Simulate work during startup
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

	cfg := PoolConfig{
		PrewarmedTargets: []string{"seq-target-1", "seq-target-2", "seq-target-3"},
	}
	pool := NewUnifiedProcessPool(cfg, mock)
	defer pool.Close()

	if err := pool.Initialize(context.Background()); err != nil {
		t.Fatalf("unexpected Initialize error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if maxConcurrentSpawns != 1 {
		t.Errorf("expected max concurrent spawns to be exactly 1, got %d", maxConcurrentSpawns)
	}
	if len(order) != 3 {
		t.Fatalf("expected 3 targets spawned, got %d", len(order))
	}
	if order[0] != "seq-target-1" || order[1] != "seq-target-2" || order[2] != "seq-target-3" {
		t.Errorf("expected sequential order [seq-target-1, seq-target-2, seq-target-3], got %v", order)
	}
}

func TestUnifiedProcessPool_Initialize_ContextCancellation(t *testing.T) {
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000010"}` + "\n"))
				_, _ = io.ReadAll(inR)
				_ = outW.Close()
			}()

			return inW, outR, errR, &MockProcessHandle{pid: 100}, nil
		},
	}

	cfg := PoolConfig{
		PrewarmedTargets: []string{"cancel-target-1", "cancel-target-2"},
	}
	pool := NewUnifiedProcessPool(cfg, mock)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled

	err := pool.Initialize(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func TestUnifiedProcessPool_ShouldRotate(t *testing.T) {
	pool := NewUnifiedProcessPool(PoolConfig{}, nil)

	// Case 1: nil daemon
	shouldRotate, reason := pool.ShouldRotate(nil)
	if shouldRotate || reason != "" {
		t.Errorf("expected false for nil daemon, got %v (%s)", shouldRotate, reason)
	}

	// Case 2: turnCount >= DefaultMaxSessionTurns
	d := &StreamingDaemon{
		turnCount: DefaultMaxSessionTurns,
		state:     StateReady,
	}
	shouldRotate, reason = pool.ShouldRotate(d)
	if !shouldRotate || !strings.Contains(reason, "turn count threshold exceeded") {
		t.Fatalf("expected rotation on turnCount >= DefaultMaxSessionTurns, got %v (%s)", shouldRotate, reason)
	}

	// Case 3: stepCount >= 180
	d.turnCount = 2
	d.stepCount = 185
	shouldRotate, reason = pool.ShouldRotate(d)
	if !shouldRotate || !strings.Contains(reason, "step count threshold exceeded") {
		t.Fatalf("expected rotation on stepCount >= 180, got %v (%s)", shouldRotate, reason)
	}

	// Case 4: below threshold
	d.turnCount = 5
	d.stepCount = 100
	shouldRotate, reason = pool.ShouldRotate(d)
	if shouldRotate || reason != "" {
		t.Errorf("expected no rotation when below thresholds, got %v (%s)", shouldRotate, reason)
	}

	// Case 5: inflight turn gating (turnCount/stepCount exceeded, but inflight > 0)
	d.turnCount = 15
	d.stepCount = 250
	d.inflight = []*TurnContext{{TurnID: "turn-inflight"}}
	shouldRotate, reason = pool.ShouldRotate(d)
	if shouldRotate || reason != "" {
		t.Errorf("expected no rotation when inflight turns exist, got %v (%s)", shouldRotate, reason)
	}

	// Case 6: dirty daemon with 0 in-flight turns
	dDirty := &StreamingDaemon{
		dirty: true,
		state: StateReady,
	}
	shouldRotate, reason = pool.ShouldRotate(dDirty)
	if !shouldRotate || reason != "daemon marked dirty during in-flight turn" {
		t.Fatalf("expected rotation for dirty daemon with 0 inflight, got %v (%s)", shouldRotate, reason)
	}

	// Case 7: dirty daemon with >0 in-flight turns
	dDirtyInflight := &StreamingDaemon{
		dirty:    true,
		state:    StateExecuting,
		inflight: []*TurnContext{{TurnID: "turn-inflight"}},
	}
	shouldRotate, reason = pool.ShouldRotate(dDirtyInflight)
	if shouldRotate || reason != "" {
		t.Fatalf("expected no rotation for dirty daemon with >0 inflight, got %v (%s)", shouldRotate, reason)
	}

	// Case 8: SessionManager transcript step count limit exceeded
	tmpDir := t.TempDir()
	sessMgr := session.New(tmpDir, tmpDir)
	pool.SetSessionManager(sessMgr)

	sessIDSteps := "sess-rotate-steps"
	sessDirSteps := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessIDSteps, ".system_generated", "logs")
	if err := os.MkdirAll(sessDirSteps, 0755); err != nil {
		t.Fatalf("failed to create logs dir: %v", err)
	}
	transcriptContent := fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\n", DefaultMaxSessionSteps-1)
	if err := os.WriteFile(filepath.Join(sessDirSteps, "transcript.jsonl"), []byte(transcriptContent), 0644); err != nil {
		t.Fatalf("failed to write transcript: %v", err)
	}

	dSessSteps := &StreamingDaemon{
		sessionID: sessIDSteps,
		state:     StateReady,
	}
	shouldRotate, reason = pool.ShouldRotate(dSessSteps)
	if !shouldRotate || !strings.Contains(reason, "transcript step count threshold exceeded") {
		t.Fatalf("expected rotation on transcript steps exceeded, got %v (%s)", shouldRotate, reason)
	}

	// Case 9: SessionManager transcript file size limit exceeded
	sessIDSize := "sess-rotate-size"
	sessDirSize := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessIDSize, ".system_generated", "logs")
	if err := os.MkdirAll(sessDirSize, 0755); err != nil {
		t.Fatalf("failed to create logs dir: %v", err)
	}
	largeData := make([]byte, session.DefaultMaxTranscriptBytes+10)
	if err := os.WriteFile(filepath.Join(sessDirSize, "transcript.jsonl"), largeData, 0644); err != nil {
		t.Fatalf("failed to write large transcript: %v", err)
	}

	dSessSize := &StreamingDaemon{
		sessionID: sessIDSize,
		state:     StateReady,
	}
	shouldRotate, reason = pool.ShouldRotate(dSessSize)
	if !shouldRotate || !strings.Contains(reason, "transcript file size threshold exceeded") {
		t.Fatalf("expected rotation on transcript size exceeded, got %v (%s)", shouldRotate, reason)
	}

	// Case 10: SessionManager DB size limit exceeded
	sessIDDB := "sess-rotate-db"
	convDir := filepath.Join(tmpDir, "conversations")
	if err := os.MkdirAll(convDir, 0755); err != nil {
		t.Fatalf("failed to create conversations dir: %v", err)
	}
	largeDBData := make([]byte, session.DefaultMaxSessionDBBytes+10)
	if err := os.WriteFile(filepath.Join(convDir, sessIDDB+".db"), largeDBData, 0644); err != nil {
		t.Fatalf("failed to write large session DB: %v", err)
	}

	dSessDB := &StreamingDaemon{
		sessionID: sessIDDB,
		state:     StateReady,
	}
	shouldRotate, reason = pool.ShouldRotate(dSessDB)
	if !shouldRotate || !strings.Contains(reason, "session DB size threshold exceeded") {
		t.Fatalf("expected rotation on session DB size exceeded, got %v (%s)", shouldRotate, reason)
	}

	// Case 11: SessionManager transcript turn count limit exceeded
	sessIDTurns := "sess-rotate-turns"
	sessDirTurns := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessIDTurns, ".system_generated", "logs")
	if err := os.MkdirAll(sessDirTurns, 0755); err != nil {
		t.Fatalf("failed to create logs dir: %v", err)
	}
	var turnContent strings.Builder
	for i := 0; i < DefaultMaxSessionTurns; i++ {
		turnContent.WriteString(fmt.Sprintf(`{"step_index": %d, "source": "USER_EXPLICIT", "type": "USER_INPUT", "content": "Turn %d"}` + "\n", i*2, i+1))
		turnContent.WriteString(fmt.Sprintf(`{"step_index": %d, "source": "MODEL", "type": "PLANNER_RESPONSE", "content": "Resp %d"}` + "\n", i*2+1, i+1))
	}
	if err := os.WriteFile(filepath.Join(sessDirTurns, "transcript.jsonl"), []byte(turnContent.String()), 0644); err != nil {
		t.Fatalf("failed to write transcript turns: %v", err)
	}

	dSessTurns := &StreamingDaemon{
		sessionID: sessIDTurns,
		state:     StateReady,
	}
	shouldRotate, reason = pool.ShouldRotate(dSessTurns)
	if !shouldRotate || !strings.Contains(reason, "transcript turn count threshold exceeded") {
		t.Fatalf("expected rotation on transcript turns exceeded, got %v (%s)", shouldRotate, reason)
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

	// nil pool safety
	var nilPool *UnifiedProcessPool
	nilPool.SetSessionManager(sm)
}

func TestUnifiedProcessPool_GetOrCreate_PreflightRotationAndGuardrails(t *testing.T) {
	var spawnCounter atomic.Int32
	var lastSpawnedSessionID atomic.Pointer[string]

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			count := spawnCounter.Add(1)
			sID := cfg.SessionID
			lastSpawnedSessionID.Store(&sID)
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				sessJSON := cfg.SessionID
				if sessJSON == "" {
					sessJSON = fmt.Sprintf("00000000-0000-0000-0000-%012d", count)
				}
				_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":%q}`+"\n", sessJSON)))
				_, _ = io.Copy(io.Discard, inR)
			}()

			return inW, outR, errR, &MockProcessHandle{pid: int(count * 100)}, nil
		},
	}

	tmpDir := t.TempDir()
	sessMgr := session.New(tmpDir, tmpDir)

	pool := NewUnifiedProcessPool(PoolConfig{
		SessionManager: sessMgr,
	}, mock)
	defer pool.Close()

	ctx := context.Background()

	// 1. Initial creation
	d1, err := pool.GetOrCreate(ctx, "target-1", "")
	if err != nil {
		t.Fatalf("failed creating initial daemon: %v", err)
	}

	// Make d1 exceed turn count threshold
	d1.mu.Lock()
	d1.turnCount = DefaultMaxSessionTurns
	d1.mu.Unlock()

	// 2. Next GetOrCreate should trigger pre-flight rotation
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

	// 3. Requesting a sessionID that exceeds disk guardrails
	oversizedSess := "550e8400-e29b-41d4-a716-446655440001"
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", oversizedSess, ".system_generated", "logs")
	if err := os.MkdirAll(sessDir, 0755); err != nil {
		t.Fatalf("failed to create logs dir: %v", err)
	}
	largeData := make([]byte, session.DefaultMaxTranscriptBytes+10)
	if err := os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), largeData, 0644); err != nil {
		t.Fatalf("failed to write large transcript: %v", err)
	}

	d3, err := pool.GetOrCreate(ctx, "target-oversized", oversizedSess)
	if err != nil {
		t.Fatalf("failed GetOrCreate for oversized session: %v", err)
	}
	if d3 == nil {
		t.Fatalf("expected non-nil daemon")
	}

	// The session passed to daemon config should have been reset to empty string
	lastSpawned := lastSpawnedSessionID.Load()
	if lastSpawned == nil || *lastSpawned != "" {
		t.Errorf("expected session to be reset to empty due to disk guardrails, got %v", lastSpawned)
	}

	// 4. Requesting a sessionID that exceeds DB guardrails
	oversizedDBSess := "550e8400-e29b-41d4-a716-446655440002"
	convDir := filepath.Join(tmpDir, "conversations")
	if err := os.MkdirAll(convDir, 0755); err != nil {
		t.Fatalf("failed to create conversations dir: %v", err)
	}
	largeDBData := make([]byte, session.DefaultMaxSessionDBBytes+10)
	if err := os.WriteFile(filepath.Join(convDir, oversizedDBSess+".db"), largeDBData, 0644); err != nil {
		t.Fatalf("failed to write large session DB: %v", err)
	}

	d4, err := pool.GetOrCreate(ctx, "target-oversized-db", oversizedDBSess)
	if err != nil {
		t.Fatalf("failed GetOrCreate for oversized DB session: %v", err)
	}
	if d4 == nil {
		t.Fatalf("expected non-nil daemon")
	}

	lastSpawnedDB := lastSpawnedSessionID.Load()
	if lastSpawnedDB == nil || *lastSpawnedDB != "" {
		t.Errorf("expected session to be reset to empty due to DB guardrails, got %v", lastSpawnedDB)
	}

	// 5. Requesting a sessionID that exceeds transcript step count guardrails
	oversizedStepsSess := "550e8400-e29b-41d4-a716-446655440003"
	sessDirSteps := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", oversizedStepsSess, ".system_generated", "logs")
	if err := os.MkdirAll(sessDirSteps, 0755); err != nil {
		t.Fatalf("failed to create logs dir: %v", err)
	}
	transcriptContent := fmt.Sprintf(`{"step_index": %d, "type": "PLANNER_RESPONSE"}`+"\n", DefaultMaxSessionSteps)
	if err := os.WriteFile(filepath.Join(sessDirSteps, "transcript.jsonl"), []byte(transcriptContent), 0644); err != nil {
		t.Fatalf("failed to write transcript: %v", err)
	}

	d5, err := pool.GetOrCreate(ctx, "target-oversized-steps", oversizedStepsSess)
	if err != nil {
		t.Fatalf("failed GetOrCreate for oversized steps session: %v", err)
	}
	if d5 == nil {
		t.Fatalf("expected non-nil daemon")
	}

	lastSpawnedSteps := lastSpawnedSessionID.Load()
	if lastSpawnedSteps == nil || *lastSpawnedSteps != "" {
		t.Errorf("expected session to be reset to empty due to step count guardrails, got %v", lastSpawnedSteps)
	}

	// 6. Requesting a sessionID that exceeds transcript turn count guardrails
	oversizedTurnsSess := "550e8400-e29b-41d4-a716-446655440004"
	sessDirTurnsGuard := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", oversizedTurnsSess, ".system_generated", "logs")
	if err := os.MkdirAll(sessDirTurnsGuard, 0755); err != nil {
		t.Fatalf("failed to create logs dir: %v", err)
	}
	var turnContentGuard strings.Builder
	for i := 0; i < DefaultMaxSessionTurns; i++ {
		turnContentGuard.WriteString(fmt.Sprintf(`{"step_index": %d, "source": "USER_EXPLICIT", "type": "USER_INPUT", "content": "Turn %d"}` + "\n", i*2, i+1))
		turnContentGuard.WriteString(fmt.Sprintf(`{"step_index": %d, "source": "MODEL", "type": "PLANNER_RESPONSE", "content": "Resp %d"}` + "\n", i*2+1, i+1))
	}
	if err := os.WriteFile(filepath.Join(sessDirTurnsGuard, "transcript.jsonl"), []byte(turnContentGuard.String()), 0644); err != nil {
		t.Fatalf("failed to write transcript turns: %v", err)
	}

	d6, err := pool.GetOrCreate(ctx, "target-oversized-turns", oversizedTurnsSess)
	if err != nil {
		t.Fatalf("failed GetOrCreate for oversized turns session: %v", err)
	}
	if d6 == nil {
		t.Fatalf("expected non-nil daemon")
	}

	lastSpawnedTurns := lastSpawnedSessionID.Load()
	if lastSpawnedTurns == nil || *lastSpawnedTurns != "" {
		t.Errorf("expected session to be reset to empty due to turn count guardrails, got %v", lastSpawnedTurns)
	}
}

func TestUnifiedProcessPool_RotateDaemon(t *testing.T) {
	var spawnCounter atomic.Int32

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			count := spawnCounter.Add(1)
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":"00000000-0000-0000-0000-%012d"}`+"\n", count)))
			}()
			_ = inR.Close()

			return inW, outR, errR, &MockProcessHandle{pid: int(count * 100)}, nil
		},
	}

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

	// 3. Rotate non-existent target key (creates new daemon cleanly)
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
	var spawnCounter atomic.Int32
	var failSpawn atomic.Bool

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			if failSpawn.Load() {
				return nil, nil, nil, nil, errors.New("simulated spawn failure")
			}
			count := spawnCounter.Add(1)
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":"00000000-0000-0000-0000-%012d"}`+"\n", count)))
			}()
			_ = inR.Close()

			return inW, outR, errR, &MockProcessHandle{pid: int(count * 100), killErr: errors.New("kill failed")}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	ctx := context.Background()

	// Daemon with failing handle.Kill() should still rotate and log warning without failing
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

	// When spawn fails during rotation, RotateDaemon returns formatted error
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

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()
			go func() {
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000099"}` + "\n"))
				_, _ = io.ReadAll(inR)
				_ = outW.Close()
			}()
			return inW, outR, errR, &MockProcessHandle{pid: 99}, nil
		},
	}

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
}

func (s *mockSinkCapture) OnTurnStarted() {}
func (s *mockSinkCapture) OnThinking()    { s.thinking = true }
func (s *mockSinkCapture) OnToolCall(tool, cmd string) {
	s.toolName = tool
	s.commandName = cmd
}
func (s *mockSinkCapture) OnToolCompleted(tool, server string, d time.Duration, status string) {}
func (s *mockSinkCapture) OnSkillActivated(skill, source string)                               {}
func (s *mockSinkCapture) OnTextDelta(delta string) { s.delta += delta }
func (s *mockSinkCapture) OnResult(res *TurnResult) { s.result = res }
func (s *mockSinkCapture) OnError(err error)        { s.err = err }

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

	sink := &mockSinkCapture{}
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

	time.Sleep(50 * time.Millisecond)

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
	sinkErr := &mockSinkCapture{}
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

	time.Sleep(50 * time.Millisecond)
	if sinkErr.err == nil || !strings.Contains(sinkErr.err.Error(), "custom failure message") {
		t.Errorf("expected custom failure message error, got %v", sinkErr.err)
	}

	// 5. Test empty error string default
	sinkErrDef := &mockSinkCapture{}
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

	time.Sleep(50 * time.Millisecond)
	if sinkErrDef.err == nil || !strings.Contains(sinkErrDef.err.Error(), "daemon execution failed") {
		t.Errorf("expected daemon execution failed error, got %v", sinkErrDef.err)
	}
}

func TestUnifiedProcessPool_TargetModelsIgnored(t *testing.T) {
	var capturedModel string
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			capturedModel = cfg.Model
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()
			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000001"}` + "\n"))
			}()
			_ = inR.Close()
			return inW, outR, errR, &MockProcessHandle{pid: 200}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{
		Model: "primary-model",
		TargetModels: map[string]string{
			"ephemeral:classifier": "flash-model",
		},
	}, mock)
	defer pool.Close()

	// 1. Target with custom TargetModels is ignored; daemon always inherits pool.Model()
	_, err := pool.GetOrCreate(context.Background(), "ephemeral:classifier", "")
	if err != nil {
		t.Fatalf("unexpected error getting daemon: %v", err)
	}
	if capturedModel != "primary-model" {
		t.Errorf("expected model primary-model (TargetModels ignored), got %q", capturedModel)
	}

	// 2. Standard target also uses pool.Model()
	_, err = pool.GetOrCreate(context.Background(), "standard-target", "")
	if err != nil {
		t.Fatalf("unexpected error getting standard daemon: %v", err)
	}
	if capturedModel != "primary-model" {
		t.Errorf("expected model primary-model, got %q", capturedModel)
	}
}

func TestUnifiedProcessPool_ExecuteEphemeral_Success(t *testing.T) {
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000002"}` + "\n"))

				buf := make([]byte, 1024)
				n, _ := inR.Read(buf)
				if n > 0 {
					_, _ = outW.Write([]byte(`{"event":"result","result":{"status":"SUCCESS","response":"ephemeral result ok"}}` + "\n"))
				}
			}()

			return inW, outR, errR, &MockProcessHandle{pid: 201}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{DefaultModel: "test-model"}, mock)
	defer pool.Close()

	res, err := pool.ExecuteEphemeral(context.Background(), "ephemeral:classifier", "test prompt")
	if err != nil {
		t.Fatalf("unexpected error from ExecuteEphemeral: %v", err)
	}
	if res != "ephemeral result ok" {
		t.Errorf("expected 'ephemeral result ok', got %q", res)
	}
}

func TestUnifiedProcessPool_ExecuteEphemeral_NilPoolAndInvalidKey(t *testing.T) {
	var nilPool *UnifiedProcessPool
	if _, err := nilPool.ExecuteEphemeral(context.Background(), "key", "prompt"); err == nil {
		t.Error("expected error from nil pool ExecuteEphemeral")
	}

	mock := &MockDaemonSpawner{}
	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	if _, err := pool.ExecuteEphemeral(context.Background(), "", "prompt"); err == nil {
		t.Error("expected error from empty targetKey")
	}
	if _, err := pool.ExecuteEphemeral(context.Background(), "   ", "prompt"); err == nil {
		t.Error("expected error from whitespace targetKey")
	}
}

func TestUnifiedProcessPool_ExecuteEphemeral_ContextCancellation(t *testing.T) {
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()
			go func() {
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000003"}` + "\n"))
			}()
			go func() {
				_, _ = io.ReadAll(inR)
			}()
			return inW, outR, errR, &MockProcessHandle{pid: 202}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	// 1. Context already cancelled before execution
	ctxCancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pool.ExecuteEphemeral(ctxCancelled, "target", "prompt"); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	// 2. Context cancelled while awaiting result
	ctxTimeout, cancelTimeout := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelTimeout()
	if _, err := pool.ExecuteEphemeral(ctxTimeout, "target", "prompt"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestUnifiedProcessPool_ExecuteEphemeral_DaemonErrors(t *testing.T) {
	// 1. Daemon spawn error
	mockSpawnErr := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return nil, nil, nil, nil, errors.New("daemon spawn failed")
		},
	}
	pool1 := NewUnifiedProcessPool(PoolConfig{}, mockSpawnErr)
	defer pool1.Close()
	if _, err := pool1.ExecuteEphemeral(context.Background(), "t1", "p1"); err == nil {
		t.Error("expected error on spawn failure")
	}

	// 2. Closed pool error
	closedPool := NewUnifiedProcessPool(PoolConfig{}, &MockDaemonSpawner{})
	_ = closedPool.Close()
	if _, err := closedPool.ExecuteEphemeral(context.Background(), "t2", "p2"); err == nil {
		t.Error("expected error on closed pool")
	}

	// 3. Stdin write failure (closed daemon stdin)
	mockWriteErr := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()
			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000004"}` + "\n"))
			}()
			_ = inR.Close() // Reading will fail/broken pipe on write
			_ = inW.Close()
			return inW, outR, errR, &MockProcessHandle{pid: 203}, nil
		},
	}
	pool2 := NewUnifiedProcessPool(PoolConfig{}, mockWriteErr)
	defer pool2.Close()
	if _, err := pool2.ExecuteEphemeral(context.Background(), "t3", "p3"); err == nil {
		t.Error("expected error on stdin write failure")
	}
}

func TestUnifiedProcessPool_EphemeralLLMFunc(t *testing.T) {
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000005"}` + "\n"))

				buf := make([]byte, 1024)
				n, _ := inR.Read(buf)
				if n > 0 {
					_, _ = outW.Write([]byte(`{"event":"result","result":{"status":"SUCCESS","response":"llm result ok"}}` + "\n"))
				}
			}()

			return inW, outR, errR, &MockProcessHandle{pid: 204}, nil
		},
	}

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

	// Nil pool guard in EphemeralLLMFunc
	var nilPool *UnifiedProcessPool
	nilLLMFn := nilPool.EphemeralLLMFunc("key")
	if _, err := nilLLMFn(context.Background(), "model", "prompt"); err == nil {
		t.Error("expected error calling LLMFunc from nil pool")
	}
}

func TestUnifiedProcessPool_GeminiHomeDirInjection(t *testing.T) {
	t.Run("CreatesGeminiDirAndSanitizesEnv", func(t *testing.T) {
		tmpDir := t.TempDir()
		var capturedEnv []string

		mock := &MockDaemonSpawner{
			SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
				capturedEnv = cfg.Env
				outR, outW := io.Pipe()
				inR, inW := io.Pipe()
				errR, _ := io.Pipe()

				go func() {
					defer outW.Close()
					_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000099"}` + "\n"))
				}()
				_ = inR.Close()

				return inW, outR, errR, &MockProcessHandle{pid: 301}, nil
			},
		}

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

		// Verify .gemini folder was created
		geminiPath := filepath.Join(tmpDir, ".gemini")
		if fi, statErr := os.Stat(geminiPath); statErr != nil || !fi.IsDir() {
			t.Errorf("expected .gemini directory at %s, err: %v", geminiPath, statErr)
		}

		// Verify capturedEnv contains sanitized variables
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

		if homeCount != 1 {
			t.Errorf("expected exactly 1 HOME in env, got %d", homeCount)
		}
		if userProfileCount != 1 {
			t.Errorf("expected exactly 1 USERPROFILE in env, got %d", userProfileCount)
		}
		if geminiCliHomeCount != 1 {
			t.Errorf("expected exactly 1 GEMINI_CLI_HOME in env, got %d", geminiCliHomeCount)
		}
	})

	t.Run("MkdirFailureReturnsError", func(t *testing.T) {
		tmpDir := t.TempDir()
		blockerFile := filepath.Join(tmpDir, "file_blocker")
		if err := os.WriteFile(blockerFile, []byte("blocker"), 0644); err != nil {
			t.Fatalf("failed to write blocker file: %v", err)
		}

		invalidHome := filepath.Join(blockerFile, "subhome")

		mock := &MockDaemonSpawner{}
		poolCfg := PoolConfig{
			DefaultModel:  "test-model",
			GeminiHomeDir: invalidHome,
		}

		pool := NewUnifiedProcessPool(poolCfg, mock)
		defer pool.Close()

		_, err := pool.GetOrCreate(context.Background(), "kiosk", "")
		if err == nil {
			t.Fatal("expected error from GetOrCreate when mkdir fails, got nil")
		}
		if !strings.Contains(err.Error(), "failed to create runtime home directory") {
			t.Errorf("expected error mentioning 'failed to create runtime home directory', got: %v", err)
		}
	})
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

func TestUnifiedProcessPool_ModelBound(t *testing.T) {
	mock := NewMockSpawner()
	pool := NewUnifiedProcessPool(PoolConfig{
		Model: "gemini-3.8-flash-low",
	}, mock)
	defer pool.Close()

	if pool.Model() != "gemini-3.8-flash-low" {
		t.Fatalf("expected pool model gemini-3.8-flash-low, got %s", pool.Model())
	}

	ctx := context.Background()
	daemon, err := pool.GetOrCreate(ctx, "target-1", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if daemon == nil {
		t.Fatalf("expected non-nil daemon")
	}

	mock.mu.Lock()
	spawnCfg := mock.lastSpawnCfg
	mock.mu.Unlock()

	if spawnCfg.Model != "gemini-3.8-flash-low" {
		t.Fatalf("expected spawned daemon model gemini-3.8-flash-low, got %s", spawnCfg.Model)
	}
}

func TestUnifiedProcessPool_ModelFallback(t *testing.T) {
	mock := NewMockSpawner()
	pool := NewUnifiedProcessPool(PoolConfig{
		DefaultModel: "gemini-2.5-pro",
	}, mock)
	defer pool.Close()

	if pool.Model() != "gemini-2.5-pro" {
		t.Fatalf("expected fallback model gemini-2.5-pro, got %s", pool.Model())
	}
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

func TestUnifiedProcessPool_NilPoolModelAndClose(t *testing.T) {
	var nilPool *UnifiedProcessPool
	if nilPool.Model() != "" {
		t.Errorf("expected empty string from nilPool.Model()")
	}
	if err := nilPool.Close(); err != nil {
		t.Errorf("expected nil error from nilPool.Close(), got %v", err)
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
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()
			go func() {
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"550e8400-e29b-41d4-a716-446655440000"}` + "\n"))
				_, _ = io.ReadAll(inR)
				_ = outW.Close()
			}()
			return inW, outR, errR, &MockProcessHandle{pid: 300}, nil
		},
	}

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

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			count := spawnCounter.Add(1)
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

	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	ctx := context.Background()

	// 1. ShouldRotateSession with nil session or nil pool
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

	// 2. Clean daemon does not need rotation
	if should, _ := pool.ShouldRotateSession(d1); should {
		t.Error("expected clean daemon ShouldRotateSession to return false")
	}

	// 3. Mark dirty: ShouldRotateSession reports true
	d1.MarkDirty()
	should, reason := pool.ShouldRotateSession(d1)
	if !should {
		t.Error("expected dirty daemon ShouldRotateSession to return true")
	}
	if !strings.Contains(reason, "dirty") {
		t.Errorf("expected reason to mention dirty, got: %s", reason)
	}

	// 4. Synchronously rotate session
	rotatedSess, err := pool.RotateSession(ctx, "target-turn-hook")
	if err != nil {
		t.Fatalf("unexpected RotateSession error: %v", err)
	}
	if rotatedSess == nil {
		t.Fatal("expected non-nil rotated session")
	}
	if rotatedSess == d1 {
		t.Fatal("expected rotated session to be different from old daemon")
	}
	if d1.State() != StateClosed {
		t.Errorf("expected old daemon to be closed, got state: %v", d1.State())
	}

	// Rotated daemon is registered in the pool as current
	curDaemon, ok := pool.Get("target-turn-hook")
	if !ok || curDaemon != rotatedSess {
		t.Errorf("expected rotated session to be registered in pool as current daemon")
	}
	if spawnCounter.Load() != 2 {
		t.Errorf("expected spawn count 2, got %d", spawnCounter.Load())
	}

	// Clean rotated session does not need rotation
	if should, _ := pool.ShouldRotateSession(rotatedSess); should {
		t.Error("expected newly rotated session ShouldRotateSession to return false")
	}

	// 5. Test closed pool behavior
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

func TestUnifiedProcessPool_TurnFinishedHook(t *testing.T) {
	TestUnifiedProcessPool_SessionRotator(t)
}

func TestUnifiedProcessPool_OptimisticMarkDirty(t *testing.T) {
	t.Run("IdlePrewarmedEvictedAndReplaced", func(t *testing.T) {
		var spawnCounter atomic.Int32
		mock := &MockDaemonSpawner{
			SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
				count := spawnCounter.Add(1)
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
		if err != nil {
			t.Fatalf("failed creating initial kiosk daemon: %v", err)
		}
		if spawnCounter.Load() != 1 {
			t.Fatalf("expected 1 initial spawn, got %d", spawnCounter.Load())
		}
		if d1.InflightCount() != 0 {
			t.Fatalf("expected d1 to have 0 inflight turns, got %d", d1.InflightCount())
		}

		// Optimistic rotation at MarkDirty() time
		pool.MarkDirty()

		// d1 should be closed asynchronously and evicted from pool.daemons
		deadline := time.Now().Add(2 * time.Second)
		var d2 *StreamingDaemon
		for time.Now().Before(deadline) {
			cur, ok := pool.Get("kiosk")
			if ok && cur != d1 && cur.State() != StateClosed {
				d2 = cur
				break
			}
			time.Sleep(20 * time.Millisecond)
		}

		if d2 == nil {
			t.Fatalf("timed out waiting for kiosk daemon to be asynchronously re-warmed and replaced")
		}
		if d2 == d1 {
			t.Errorf("expected d2 to be a distinct instance from d1")
		}
		if d2.IsDirty() {
			t.Errorf("expected replacement daemon d2 to not be dirty")
		}
		if d1.State() != StateClosed {
			t.Errorf("expected evicted daemon d1 to be closed, got state: %v", d1.State())
		}
		if spawnCounter.Load() < 2 {
			t.Errorf("expected at least 2 spawns, got %d", spawnCounter.Load())
		}
	})

	t.Run("IdleNonPrewarmedEvictedAndClosed", func(t *testing.T) {
		var spawnCounter atomic.Int32
		mock := &MockDaemonSpawner{
			SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
				count := spawnCounter.Add(1)
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
		d1, err := pool.GetOrCreate(ctx, "thread-nonprewarmed", "")
		if err != nil {
			t.Fatalf("failed creating initial daemon: %v", err)
		}
		if d1.InflightCount() != 0 {
			t.Fatalf("expected 0 inflight turns, got %d", d1.InflightCount())
		}

		pool.MarkDirty()

		// Daemon should be evicted from pool and closed, but NOT re-warmed
		deadline := time.Now().Add(1 * time.Second)
		for time.Now().Before(deadline) && d1.State() != StateClosed {
			time.Sleep(10 * time.Millisecond)
		}
		if d1.State() != StateClosed {
			t.Errorf("expected idle non-prewarmed daemon d1 to be closed after MarkDirty")
		}
		if _, ok := pool.Get("thread-nonprewarmed"); ok {
			t.Errorf("expected thread-nonprewarmed to be evicted from pool")
		}

		// Prewarmed target "kiosk" should be reconciled and spawned
		var kioskDaemon *StreamingDaemon
		for time.Now().Before(deadline) {
			if kd, ok := pool.Get("kiosk"); ok && kd != nil {
				kioskDaemon = kd
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if kioskDaemon == nil {
			t.Errorf("expected prewarmed kiosk to be spawned on MarkDirty")
		}

		// Exactly 2 spawns: 1 for thread-nonprewarmed, 1 for reconciled kiosk
		if spawnCounter.Load() != 2 {
			t.Errorf("expected exactly 2 spawns (initial non-prewarmed + reconciled kiosk), got %d", spawnCounter.Load())
		}
	})

	t.Run("InflightPreservedAndRotatedOnCompletion", func(t *testing.T) {
		var spawnCounter atomic.Int32
		mock := &MockDaemonSpawner{
			SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
				count := spawnCounter.Add(1)
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

		pool := NewUnifiedProcessPool(PoolConfig{}, mock)
		defer pool.Close()

		ctx := context.Background()
		d1, err := pool.GetOrCreate(ctx, "thread-inflight", "")
		if err != nil {
			t.Fatalf("failed creating initial daemon: %v", err)
		}

		// Simulate an in-flight turn
		sink := newMockTurnSink()
		turn := &TurnContext{TurnID: "t-inflight", Sink: sink, CreatedAt: time.Now()}
		d1.inflight = append(d1.inflight, turn)

		if d1.InflightCount() != 1 {
			t.Fatalf("expected 1 inflight turn, got %d", d1.InflightCount())
		}

		pool.MarkDirty()

		// Daemon must NOT be closed mid-flight and must still be tracked
		if d1.State() == StateClosed {
			t.Fatalf("in-flight daemon was closed prematurely on MarkDirty")
		}
		cur, ok := pool.Get("thread-inflight")
		if !ok || cur != d1 {
			t.Fatalf("expected d1 to remain tracked in pool during in-flight turn")
		}
		if !d1.IsDirty() {
			t.Fatalf("expected in-flight daemon to be marked dirty")
		}

		// Now finish turn via NDJSON result event
		d1.dispatchNDJSONLine(`{"event":"result","result":{"status":"SUCCESS","response":"finished"}}`)

		// SessionRotator rotates upon turn completion
		if should, _ := pool.ShouldRotateSession(d1); !should {
			t.Fatalf("expected dirty daemon to indicate rotation needed after turn completion")
		}
		rotatedSess, err := pool.RotateSession(ctx, "thread-inflight")
		if err != nil {
			t.Fatalf("failed rotating session: %v", err)
		}
		rotatedDaemon, ok := rotatedSess.(*StreamingDaemon)
		if !ok || rotatedDaemon == nil || rotatedDaemon == d1 {
			t.Fatalf("expected new rotated daemon distinct from d1")
		}
		if spawnCounter.Load() != 2 {
			t.Errorf("expected spawn count 2, got %d", spawnCounter.Load())
		}
		if d1.State() != StateClosed {
			t.Errorf("expected old daemon d1 to be closed after rotation")
		}
	})

	t.Run("RapidSuccessiveMarkDirtySuppressesDuplicatePrewarming", func(t *testing.T) {
		var concurrentSpawns atomic.Int32
		var maxConcurrentSpawns atomic.Int32
		var totalSpawns atomic.Int32
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
		if err != nil {
			t.Fatalf("failed creating initial daemon: %v", err)
		}
		if d1.InflightCount() != 0 {
			t.Fatalf("expected 0 inflight turns, got %d", d1.InflightCount())
		}

		// Fire rapid successive MarkDirty calls
		for i := 0; i < 10; i++ {
			pool.MarkDirty()
		}

		// Unblock the gated spawn
		close(spawnGate)

		// Wait for replacement daemon
		deadline := time.Now().Add(2 * time.Second)
		var d2 *StreamingDaemon
		for time.Now().Before(deadline) {
			cur, ok := pool.Get("kiosk")
			if ok && cur != d1 && cur.State() != StateClosed {
				d2 = cur
				break
			}
			time.Sleep(20 * time.Millisecond)
		}

		if d2 == nil {
			t.Fatalf("timed out waiting for kiosk daemon replacement")
		}
		if maxConcurrentSpawns.Load() > 1 {
			t.Errorf("expected at most 1 concurrent spawn during pre-warming, got %d", maxConcurrentSpawns.Load())
		}
		if totalSpawns.Load() != 2 {
			t.Errorf("expected exactly 2 total spawns (1 initial + 1 pre-warm), got %d", totalSpawns.Load())
		}
	})

	t.Run("ClosedPoolMarkDirtyReturnsEarly", func(t *testing.T) {
		pool := NewUnifiedProcessPool(PoolConfig{}, nil)
		if err := pool.Close(); err != nil {
			t.Fatalf("unexpected Close() error: %v", err)
		}
		pool.MarkDirty() // Should safely return early under lock
	})

	t.Run("EvictedDaemonCloseErrorLogged", func(t *testing.T) {
		mock := &MockDaemonSpawner{
			SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
				outR, outW := io.Pipe()
				inR, inW := io.Pipe()
				errR, _ := io.Pipe()
				go func() {
					defer outW.Close()
					_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000088"}` + "\n"))
					_, _ = io.Copy(io.Discard, inR)
				}()
				return inW, outR, errR, &MockProcessHandle{pid: 88, killErr: errors.New("simulated eviction kill error")}, nil
			},
		}

		pool := NewUnifiedProcessPool(PoolConfig{}, mock)
		defer pool.Close()

		ctx := context.Background()
		d, err := pool.GetOrCreate(ctx, "thread-evict-err", "")
		if err != nil {
			t.Fatalf("failed creating daemon: %v", err)
		}

		pool.MarkDirty()

		deadline := time.Now().Add(1 * time.Second)
		for time.Now().Before(deadline) && d.State() != StateClosed {
			time.Sleep(10 * time.Millisecond)
		}
		if d.State() != StateClosed {
			t.Errorf("expected daemon to be closed despite kill error")
		}
	})

	t.Run("RewarmingFailureLogged", func(t *testing.T) {
		var spawnCount atomic.Int32
		mock := &MockDaemonSpawner{
			SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
				count := spawnCount.Add(1)
				if count > 1 {
					return nil, nil, nil, nil, errors.New("simulated re-warming spawn failure")
				}
				outR, outW := io.Pipe()
				inR, inW := io.Pipe()
				errR, _ := io.Pipe()
				go func() {
					defer outW.Close()
					_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000077"}` + "\n"))
					_, _ = io.Copy(io.Discard, inR)
				}()
				return inW, outR, errR, &MockProcessHandle{pid: 77}, nil
			},
		}

		pool := NewUnifiedProcessPool(PoolConfig{
			PrewarmedTargets: []string{"kiosk"},
		}, mock)
		defer pool.Close()

		ctx := context.Background()
		d1, err := pool.GetOrCreate(ctx, "kiosk", "")
		if err != nil {
			t.Fatalf("failed creating initial kiosk: %v", err)
		}

		pool.MarkDirty()

		deadline := time.Now().Add(1 * time.Second)
		for time.Now().Before(deadline) && d1.State() != StateClosed {
			time.Sleep(10 * time.Millisecond)
		}
		if d1.State() != StateClosed {
			t.Errorf("expected d1 to be closed")
		}
	})
}

func TestUnifiedProcessPool_GetOrCreate_RejectsDirtyDaemon(t *testing.T) {
	t.Run("SpawnsFreshAndClosesDirty", func(t *testing.T) {
		var spawnCounter atomic.Int32
		mock := &MockDaemonSpawner{
			SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
				count := spawnCounter.Add(1)
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

		pool := NewUnifiedProcessPool(PoolConfig{}, mock)
		defer pool.Close()

		ctx := context.Background()
		d1, err := pool.GetOrCreate(ctx, "thread-dirty", "")
		if err != nil {
			t.Fatalf("initial GetOrCreate failed: %v", err)
		}

		d1.MarkDirty()
		if !d1.IsDirty() {
			t.Fatalf("expected d1 to be marked dirty")
		}

		// Calling GetOrCreate for thread-dirty must reject dirty d1, close d1, and spawn a fresh replacement d2
		d2, err := pool.GetOrCreate(ctx, "thread-dirty", "")
		if err != nil {
			t.Fatalf("second GetOrCreate failed: %v", err)
		}
		if d2 == d1 {
			t.Errorf("expected GetOrCreate to reject dirty daemon and return a fresh daemon")
		}
		if d2.IsDirty() {
			t.Errorf("expected fresh daemon d2 not to be dirty")
		}

		deadline := time.Now().Add(1 * time.Second)
		for time.Now().Before(deadline) && d1.State() != StateClosed {
			time.Sleep(10 * time.Millisecond)
		}
		if d1.State() != StateClosed {
			t.Errorf("expected dirty daemon d1 to be closed, got state: %v", d1.State())
		}
		if spawnCounter.Load() != 2 {
			t.Errorf("expected exactly 2 spawns, got %d", spawnCounter.Load())
		}
	})

	t.Run("DirtyDaemonCloseErrorLogged", func(t *testing.T) {
		mock := &MockDaemonSpawner{
			SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
				outR, outW := io.Pipe()
				inR, inW := io.Pipe()
				errR, _ := io.Pipe()
				go func() {
					defer outW.Close()
					_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000066"}` + "\n"))
					_, _ = io.Copy(io.Discard, inR)
				}()
				return inW, outR, errR, &MockProcessHandle{pid: 66, killErr: errors.New("simulated dirty daemon kill error")}, nil
			},
		}

		pool := NewUnifiedProcessPool(PoolConfig{}, mock)
		defer pool.Close()

		ctx := context.Background()
		d1, err := pool.GetOrCreate(ctx, "target-dirty-err", "")
		if err != nil {
			t.Fatalf("failed creating initial daemon: %v", err)
		}

		d1.MarkDirty()

		d2, err := pool.GetOrCreate(ctx, "target-dirty-err", "")
		if err != nil {
			t.Fatalf("failed creating replacement daemon: %v", err)
		}
		if d2 == d1 {
			t.Errorf("expected replacement daemon")
		}
	})

	t.Run("ReconcilesNewlyAddedPrewarmedTargets", func(t *testing.T) {
		var spawnCounter atomic.Int32
		mock := &MockDaemonSpawner{
			SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
				count := spawnCounter.Add(1)
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
		kioskDaemon, err := pool.GetOrCreate(ctx, "kiosk", "")
		if err != nil {
			t.Fatalf("failed creating initial kiosk daemon: %v", err)
		}
		if spawnCounter.Load() != 1 {
			t.Fatalf("expected 1 initial spawn, got %d", spawnCounter.Load())
		}

		pool.UpdatePrewarmedTargets([]string{"touch-kiosk-kitchen"})
		pool.MarkDirty()
		pool.WaitBackground()

		// 1. "kiosk" should be evicted and closed
		if kioskDaemon.State() != StateClosed {
			t.Errorf("expected removed prewarmed target kiosk to be closed, got state: %v", kioskDaemon.State())
		}
		if _, ok := pool.Get("kiosk"); ok {
			t.Errorf("expected kiosk to be evicted from pool")
		}

		// 2. "touch-kiosk-kitchen" should be eagerly prewarmed
		newDaemon, ok := pool.Get("touch-kiosk-kitchen")
		if !ok || newDaemon == nil || newDaemon.State() == StateClosed {
			t.Fatalf("expected active touch-kiosk-kitchen daemon in pool")
		}
		if newDaemon.IsDirty() {
			t.Errorf("expected touch-kiosk-kitchen daemon to not be dirty")
		}
	})
}

func TestUnifiedProcessPool_PrewarmedTargets_DynamicUpdate(t *testing.T) {
	t.Run("NilPool_ReturnsNil", func(t *testing.T) {
		var p *UnifiedProcessPool
		if got := p.PrewarmedTargets(); got != nil {
			t.Errorf("expected nil from nil pool, got %v", got)
		}
		p.UpdatePrewarmedTargets([]string{"test"}) // Should not panic
	})

	t.Run("InitialAndDefensiveCopy", func(t *testing.T) {
		pool := NewUnifiedProcessPool(PoolConfig{
			PrewarmedTargets: []string{"a", "b"},
		}, &MockDaemonSpawner{})
		defer pool.Close()

		targets := pool.PrewarmedTargets()
		if len(targets) != 2 || targets[0] != "a" || targets[1] != "b" {
			t.Fatalf("unexpected targets: %v", targets)
		}

		targets[0] = "mutated"
		fresh := pool.PrewarmedTargets()
		if fresh[0] != "a" {
			t.Errorf("defensive copy violated: pool target mutated to %q", fresh[0])
		}
	})

	t.Run("UpdateFiltersWhitespaceAndEmpty", func(t *testing.T) {
		pool := NewUnifiedProcessPool(PoolConfig{}, &MockDaemonSpawner{})
		defer pool.Close()

		pool.UpdatePrewarmedTargets([]string{"  target1  ", "", "   ", "target2"})
		targets := pool.PrewarmedTargets()
		if len(targets) != 2 || targets[0] != "target1" || targets[1] != "target2" {
			t.Fatalf("expected [target1, target2], got %v", targets)
		}

		pool.UpdatePrewarmedTargets(nil)
		if got := pool.PrewarmedTargets(); got != nil {
			t.Errorf("expected nil after setting nil targets, got %v", got)
		}
	})

	t.Run("UpdateDeduplicatesTargets", func(t *testing.T) {
		pool := NewUnifiedProcessPool(PoolConfig{}, &MockDaemonSpawner{})
		defer pool.Close()

		pool.UpdatePrewarmedTargets([]string{"kiosk", "kiosk", "touch-kiosk-kitchen", "  kiosk  "})
		targets := pool.PrewarmedTargets()
		if len(targets) != 2 || targets[0] != "kiosk" || targets[1] != "touch-kiosk-kitchen" {
			t.Fatalf("expected [kiosk, touch-kiosk-kitchen], got %v", targets)
		}
	})
}

func TestUnifiedProcessPool_GetOrCreate_PropagatesSessionID(t *testing.T) {
	var capturedCfg DaemonConfig
	var spawnCount atomic.Int32

	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			spawnCount.Add(1)
			capturedCfg = cfg
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				sessID := cfg.SessionID
				if sessID == "" {
					sessID = "550e8400-e29b-41d4-a716-446655440000"
				}
				_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":%q}`+"\n", sessID)))
				_, _ = io.Copy(io.Discard, inR)
			}()

			return inW, outR, errR, &MockProcessHandle{pid: 200}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	ctx := context.Background()

	// 1. Non-empty session ID should be passed to DaemonConfig and daemon
	customSessID := "11111111-2222-3333-4444-555555555555"
	d1, err := pool.GetOrCreate(ctx, "target-prop", customSessID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedCfg.SessionID != customSessID {
		t.Errorf("expected DaemonConfig.SessionID %q, got %q", customSessID, capturedCfg.SessionID)
	}
	if d1.SessionID() != customSessID {
		t.Errorf("expected daemon SessionID %q, got %q", customSessID, d1.SessionID())
	}

	// 2. Calling GetOrCreate with same session ID reuses existing live daemon
	d1Repeat, err := pool.GetOrCreate(ctx, "target-prop", customSessID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d1Repeat != d1 {
		t.Errorf("expected same daemon instance returned")
	}
	if spawnCount.Load() != 1 {
		t.Errorf("expected exactly 1 spawn, got %d", spawnCount.Load())
	}

	// 3. Empty session ID on new target creates fresh session
	d2, err := pool.GetOrCreate(ctx, "target-fresh", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedCfg.SessionID != "" {
		t.Errorf("expected empty DaemonConfig.SessionID for fresh target, got %q", capturedCfg.SessionID)
	}
	if d2.SessionID() != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("expected fresh session ID from mock, got %q", d2.SessionID())
	}
}

func TestUnifiedProcessPool_GetOrCreate_MismatchedSessionIDRotates(t *testing.T) {
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				sessID := cfg.SessionID
				if sessID == "" {
					sessID = "550e8400-e29b-41d4-a716-446655440000"
				}
				_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":%q}`+"\n", sessID)))
				_, _ = io.Copy(io.Discard, inR)
			}()

			return inW, outR, errR, &MockProcessHandle{pid: 201}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	ctx := context.Background()

	sess1 := "11111111-1111-1111-1111-111111111111"
	d1, err := pool.GetOrCreate(ctx, "target-switch", sess1)
	if err != nil {
		t.Fatalf("unexpected error spawning d1: %v", err)
	}
	if d1.SessionID() != sess1 {
		t.Fatalf("expected d1 session %q, got %q", sess1, d1.SessionID())
	}

	// Calling with different session ID should evict and close d1, returning d2
	sess2 := "22222222-2222-2222-2222-222222222222"
	d2, err := pool.GetOrCreate(ctx, "target-switch", sess2)
	if err != nil {
		t.Fatalf("unexpected error spawning d2: %v", err)
	}
	if d2 == d1 {
		t.Errorf("expected new daemon instance after session mismatch")
	}
	if d2.SessionID() != sess2 {
		t.Errorf("expected d2 session %q, got %q", sess2, d2.SessionID())
	}

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) && d1.State() != StateClosed {
		time.Sleep(10 * time.Millisecond)
	}
	if d1.State() != StateClosed {
		t.Errorf("expected old daemon d1 to be closed after session mismatch, got %v", d1.State())
	}
}

func TestUnifiedProcessPool_GetOrCreateSession_PropagatesSessionID(t *testing.T) {
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()

			go func() {
				defer outW.Close()
				sessID := cfg.SessionID
				if sessID == "" {
					sessID = "550e8400-e29b-41d4-a716-446655440000"
				}
				_, _ = outW.Write([]byte(fmt.Sprintf(`{"event":"init","session_id":%q}`+"\n", sessID)))
				_, _ = io.Copy(io.Discard, inR)
			}()

			return inW, outR, errR, &MockProcessHandle{pid: 202}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	ctx := context.Background()
	customSessID := "33333333-3333-3333-3333-333333333333"
	sess, err := pool.GetOrCreateSession(ctx, "target-sess", customSessID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sess.SessionID() != customSessID {
		t.Errorf("expected session ID %q, got %q", customSessID, sess.SessionID())
	}
}

