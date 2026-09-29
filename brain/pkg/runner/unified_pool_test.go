package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
				_, _ = io.Copy(io.Discard, inR)
			}()

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

	d, err := pool.GetOrCreate(context.Background(), "kiosk")
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
	_, err := pool.GetOrCreate(context.Background(), "ephemeral:classifier")
	if err != nil {
		t.Fatalf("unexpected error getting daemon: %v", err)
	}
	if capturedModel != "primary-model" {
		t.Errorf("expected model primary-model (TargetModels ignored), got %q", capturedModel)
	}

	// 2. Standard target also uses pool.Model()
	_, err = pool.GetOrCreate(context.Background(), "standard-target")
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

		_, err := pool.GetOrCreate(context.Background(), "kiosk")
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

		_, err := pool.GetOrCreate(context.Background(), "kiosk")
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
	daemon, err := pool.GetOrCreate(ctx, "target-1")
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

	d1, err1 := pool.GetOrCreate(ctx, "target-a")
	d2, err2 := pool.GetOrCreate(ctx, "target-b")
	d3, err3 := pool.GetOrCreate(ctx, "target-c")

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

func TestUnifiedProcessPool_TurnCompletionRotationHook(t *testing.T) {
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
	d1, err := pool.GetOrCreate(ctx, "target-turn-hook")
	if err != nil {
		t.Fatalf("failed creating initial daemon: %v", err)
	}

	// 1. If not dirty, onTurnFinished callback does not rotate
	sink1 := newMockTurnSink()
	turn1 := &TurnContext{TurnID: "t-1", Sink: sink1, CreatedAt: time.Now()}
	d1.inflight = append(d1.inflight, turn1)
	d1.dispatchNDJSONLine(`{"event":"result","result":{"status":"SUCCESS","response":"ok"}}`)
	time.Sleep(50 * time.Millisecond)

	curDaemon, ok := pool.Get("target-turn-hook")
	if !ok || curDaemon != d1 {
		t.Fatalf("expected d1 to still be current when not dirty")
	}

	// 2. Mark dirty and dispatch turn completion
	d1.MarkDirty()
	sink2 := newMockTurnSink()
	turn2 := &TurnContext{TurnID: "t-2", Sink: sink2, CreatedAt: time.Now()}
	d1.inflight = append(d1.inflight, turn2)
	d1.dispatchNDJSONLine(`{"event":"result","result":{"status":"SUCCESS","response":"ok"}}`)

	// Wait for background rotation to replace d1
	var rotatedDaemon *StreamingDaemon
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		d, ok := pool.Get("target-turn-hook")
		if ok && d != d1 && d.State() != StateClosed {
			rotatedDaemon = d
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if rotatedDaemon == nil {
		t.Fatalf("timed out waiting for background rotation on turn completion")
	}
	if spawnCounter.Load() != 2 {
		t.Errorf("expected spawn count 2, got %d", spawnCounter.Load())
	}

	// 3. Verify onTurnFinished ignores closed pool
	if err := pool.Close(); err != nil {
		t.Fatalf("unexpected pool.Close() error: %v", err)
	}
	// Calling onTurnFinished on rotatedDaemon after pool closed should return early safely
	rotatedDaemon.MarkDirty()
	rotatedDaemon.onTurnFinished(rotatedDaemon)
}




