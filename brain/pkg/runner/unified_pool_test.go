package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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
				defer outW.Close()
				_, _ = outW.Write([]byte(`{"event":"init","session_id":"00000000-0000-0000-0000-000000000010"}` + "\n"))
			}()
			_ = inR.Close()

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
	d1, err1 := pool.GetOrCreate(context.Background(), "kiosk")
	d2, err2 := pool.GetOrCreate(context.Background(), "kiosk")

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
			}()
			_ = inR.Close()

			return inW, outR, errR, &MockProcessHandle{pid: 200}, nil
		},
	}

	pool := NewUnifiedProcessPool(PoolConfig{}, mock)
	defer pool.Close()

	d1, err := pool.GetOrCreate(context.Background(), "thread-1")
	if err != nil {
		t.Fatalf("first GetOrCreate failed: %v", err)
	}

	// Reusing live daemon
	d2, err := pool.GetOrCreate(context.Background(), "thread-1")
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
	d3, err := pool.GetOrCreate(context.Background(), "thread-1")
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

	d, err := pool.GetOrCreate(context.Background(), "failed-target")
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

	d1, err1 := pool.GetOrCreate(context.Background(), "thread-alpha")
	d2, err2 := pool.GetOrCreate(context.Background(), "device-voice")

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

	if _, err := pool.GetOrCreate(context.Background(), "any"); err == nil {
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
		_, err := pool.GetOrCreate(context.Background(), "slow-target")
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
	_, err := pool.GetOrCreate(context.Background(), "error-target")
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
	// Give background pre-warm goroutine time to run and log warning
	time.Sleep(50 * time.Millisecond)
}

func TestUnifiedProcessPool_ShouldRotate(t *testing.T) {
	pool := NewUnifiedProcessPool(PoolConfig{}, nil)

	// Case 1: nil daemon
	shouldRotate, reason := pool.ShouldRotate(nil)
	if shouldRotate || reason != "" {
		t.Errorf("expected false for nil daemon, got %v (%s)", shouldRotate, reason)
	}

	// Case 2: turnCount >= 10
	d := &StreamingDaemon{
		turnCount: 10,
		state:     StateReady,
	}
	shouldRotate, reason = pool.ShouldRotate(d)
	if !shouldRotate || !strings.Contains(reason, "turn count threshold exceeded") {
		t.Fatalf("expected rotation on turnCount >= 10, got %v (%s)", shouldRotate, reason)
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
	d1, err := pool.GetOrCreate(ctx, "target-session")
	if err != nil {
		t.Fatalf("failed creating initial daemon: %v", err)
	}
	d1.mu.Lock()
	d1.turnCount = 10
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
	d1, err := pool.GetOrCreate(ctx, "kill-err-target")
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
