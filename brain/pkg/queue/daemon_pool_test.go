package queue

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/runner"
)

func TestDaemonPool_BoundedCeilingAndLRUEviction(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_agy.sh")
	_ = os.WriteFile(mockBin, []byte("#!/bin/sh\nwhile true; do sleep 1; done\n"), 0755)

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.MaxConcurrentDaemons = 2
		d.DaemonIdleTimeout = 1 * time.Second
	})

	pool := NewDaemonPool(cfg, mockBin, nil, tempDir)
	defer pool.Close()

	ctx := context.Background()

	_, err := pool.GetOrCreateDaemon(ctx, "thread-1", "sess-1", "model-a")
	if err != nil {
		t.Fatalf("failed to create daemon 1: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	_, err = pool.GetOrCreateDaemon(ctx, "thread-2", "sess-2", "model-a")
	if err != nil {
		t.Fatalf("failed to create daemon 2: %v", err)
	}

	if pool.ActiveCount() != 2 {
		t.Errorf("expected 2 active daemons, got %d", pool.ActiveCount())
	}

	// Spawn 3: should trigger LRU eviction of thread-1
	_, err = pool.GetOrCreateDaemon(ctx, "thread-3", "sess-3", "model-a")
	if err != nil {
		t.Fatalf("failed to create daemon 3: %v", err)
	}

	if pool.ActiveCount() > 2 {
		t.Errorf("expected count bounded by 2, got %d", pool.ActiveCount())
	}
	if pool.HasDaemon("thread-1") {
		t.Errorf("expected thread-1 to be evicted by LRU sweep")
	}
	// All daemons have active tasks: capacity warning branch
	d2, _ := pool.GetOrCreateDaemon(ctx, "thread-2", "sess-2", "model-a")
	d3, _ := pool.GetOrCreateDaemon(ctx, "thread-3", "sess-3", "model-a")
	d2.TaskTracker().Add(runner.TaskMetadata{TaskID: "task-2", StartedAt: time.Now()})
	d3.TaskTracker().Add(runner.TaskMetadata{TaskID: "task-3", StartedAt: time.Now()})
	_, _ = pool.GetOrCreateDaemon(ctx, "thread-4", "sess-4", "model-a")
}

func TestDaemonPool_MemoryPressureLRUEviction(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_agy.sh")
	_ = os.WriteFile(mockBin, []byte("#!/bin/sh\nwhile true; do sleep 1; done\n"), 0755)

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.MaxConcurrentDaemons = 10
	})
	pool := NewDaemonPool(cfg, mockBin, nil, tempDir)
	defer pool.Close()

	// Mock memory checker returning 10% available (triggering memory pressure eviction)
	pool.SetMemoryChecker(func() (float64, error) {
		return 0.10, nil
	})

	ctx := context.Background()
	_, err := pool.GetOrCreateDaemon(ctx, "thread-mem-1", "sess-1", "model-a")
	if err != nil {
		t.Fatalf("failed to create daemon: %v", err)
	}

	evicted := pool.CheckMemoryPressureAndEvict()
	if !evicted {
		t.Errorf("expected eviction under 10%% memory headroom")
	}
	if pool.HasDaemon("thread-mem-1") {
		t.Errorf("expected thread-mem-1 to be evicted due to memory pressure")
	}
}

func TestDaemonPool_MonitorZombieTasks(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_agy.sh")
	_ = os.WriteFile(mockBin, []byte("#!/bin/sh\nwhile true; do sleep 1; done\n"), 0755)

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.MaxBackgroundTaskDuration = 50 * time.Millisecond
	})
	pool := NewDaemonPool(cfg, mockBin, nil, tempDir)
	defer pool.Close()

	ctx := context.Background()
	d, err := pool.GetOrCreateDaemon(ctx, "thread-zombie", "sess-zombie", "model-a")
	if err != nil {
		t.Fatalf("failed to create daemon: %v", err)
	}

	// Register task started 100ms ago
	d.TaskTracker().Add(runner.TaskMetadata{
		TaskID:    "zombie-task-1",
		ToolName:  "run_command",
		StartedAt: time.Now().Add(-100 * time.Millisecond),
	})

	if !pool.HasActiveTasks("thread-zombie") {
		t.Errorf("expected active tasks for thread-zombie")
	}

	pruned := pool.MonitorZombieTasks(50 * time.Millisecond)
	if pruned != 1 {
		t.Errorf("expected 1 zombie task pruned, got %d", pruned)
	}
	if d.TaskTracker().Has("zombie-task-1") {
		t.Errorf("expected zombie task to be dereferenced from TaskTracker")
	}
	if pool.HasActiveTasks("thread-zombie") {
		t.Errorf("expected 0 active tasks for thread-zombie after prune")
	}
}

func TestDaemonPool_IdlePruningAndDirtyReload(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_agy.sh")
	_ = os.WriteFile(mockBin, []byte("#!/bin/sh\nwhile true; do sleep 1; done\n"), 0755)

	cfg := config.NewTestConfig()
	pool := NewDaemonPool(cfg, mockBin, nil, tempDir)
	defer pool.Close()

	ctx := context.Background()
	_, _ = pool.GetOrCreateDaemon(ctx, "thread-idle-1", "sess-1", "model-a")
	d2, _ := pool.GetOrCreateDaemon(ctx, "thread-idle-2", "sess-2", "model-a")
	// Add active task to d2 so it's not idle
	d2.TaskTracker().Add(runner.TaskMetadata{TaskID: "task-dirty-active", StartedAt: time.Now()})

	// MarkDirty should prune idle daemons (thread-idle-1) and mark active daemons (thread-idle-2) dirty
	pool.MarkDirty()
	if pool.HasDaemon("thread-idle-1") {
		t.Errorf("expected idle daemon thread-idle-1 to be pruned")
	}
	if !pool.HasDaemon("thread-idle-2") {
		t.Errorf("expected active daemon thread-idle-2 to remain")
	}
	if !d2.IsDirty() {
		t.Errorf("expected thread-idle-2 to be marked dirty")
	}
}

func TestDaemonPool_ReleaseDaemonAndPruneIdle(t *testing.T) {
	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_agy.sh")
	_ = os.WriteFile(mockBin, []byte("#!/bin/sh\nwhile true; do sleep 1; done\n"), 0755)

	cfg := config.NewTestConfig()
	pool := NewDaemonPool(cfg, mockBin, nil, tempDir)
	defer pool.Close()

	ctx := context.Background()
	d, err := pool.GetOrCreateDaemon(ctx, "thread-rel", "sess-1", "model-a")
	if err != nil {
		t.Fatalf("failed to create daemon: %v", err)
	}

	// ReleaseDaemon on non-dirty daemon should be no-op
	pool.ReleaseDaemon("thread-rel")
	if !pool.HasDaemon("thread-rel") {
		t.Errorf("expected daemon to remain when not dirty")
	}

	// ReleaseDaemon when dirty but has active tasks should NOT release
	d.SetDirty(true)
	d.TaskTracker().Add(runner.TaskMetadata{TaskID: "task-active", StartedAt: time.Now()})
	pool.ReleaseDaemon("thread-rel")
	if !pool.HasDaemon("thread-rel") {
		t.Errorf("expected daemon to remain when dirty but has active tasks")
	}

	// ReleaseDaemon when dirty and 0 active tasks should close and remove
	d.TaskTracker().Remove("task-active")
	pool.ReleaseDaemon("thread-rel")
	if pool.HasDaemon("thread-rel") {
		t.Errorf("expected daemon to be released and removed")
	}

	// Test PruneIdle
	d2, _ := pool.GetOrCreateDaemon(ctx, "thread-prune", "sess-2", "model-a")
	_ = d2
	time.Sleep(10 * time.Millisecond)
	pruned := pool.PruneIdle(5 * time.Millisecond)
	if pruned != 1 {
		t.Errorf("expected 1 daemon pruned, got %d", pruned)
	}
	if pool.HasDaemon("thread-prune") {
		t.Errorf("expected thread-prune to be pruned")
	}

	// Test EvictLRU directly
	pool.GetOrCreateDaemon(ctx, "thread-lru", "sess-3", "model-a")
	if !pool.EvictLRU() {
		t.Errorf("expected EvictLRU to succeed")
	}
	if pool.HasDaemon("thread-lru") {
		t.Errorf("expected thread-lru to be evicted")
	}
}

func TestDaemonPool_MemoryCheckerAndSessionRecreation(t *testing.T) {
	// Test DefaultLinuxMemoryChecker execution
	avail, err := DefaultLinuxMemoryChecker()
	if err == nil {
		if avail < 0.0 || avail > 1.0 {
			t.Errorf("expected memory fraction between 0.0 and 1.0, got %f", avail)
		}
	}

	tempDir := t.TempDir()
	mockBin := filepath.Join(tempDir, "mock_agy.sh")
	_ = os.WriteFile(mockBin, []byte("#!/bin/sh\nwhile true; do sleep 1; done\n"), 0755)

	cfg := config.NewTestConfig()
	pool := NewDaemonPool(cfg, mockBin, nil, tempDir)
	defer pool.Close()

	// Memory checker returning error or high headroom
	pool.SetMemoryChecker(func() (float64, error) {
		return 0.50, nil
	})
	if pool.CheckMemoryPressureAndEvict() {
		t.Errorf("expected false when memory headroom is 50%%")
	}

	pool.SetMemoryChecker(func() (float64, error) {
		return 0.0, os.ErrNotExist
	})
	if pool.CheckMemoryPressureAndEvict() {
		t.Errorf("expected false when memory checker returns error")
	}

	pool.SetMemoryChecker(nil)
	if pool.CheckMemoryPressureAndEvict() {
		t.Errorf("expected false when memory checker is nil")
	}

	// Test session recreation when session ID changes
	ctx := context.Background()
	d1, err := pool.GetOrCreateDaemon(ctx, "thread-recreate", "sess-alpha", "model-a")
	if err != nil {
		t.Fatalf("failed to create daemon 1: %v", err)
	}
	if d1.SessionID() != "sess-alpha" {
		t.Errorf("expected session sess-alpha, got %s", d1.SessionID())
	}

	// Same session ID returns same daemon
	dSame, err := pool.GetOrCreateDaemon(ctx, "thread-recreate", "sess-alpha", "model-a")
	if err != nil || dSame != d1 {
		t.Errorf("expected same daemon returned for same session ID")
	}

	// Different session ID closes old daemon and creates new one
	d2, err := pool.GetOrCreateDaemon(ctx, "thread-recreate", "sess-beta", "model-a")
	if err != nil {
		t.Fatalf("failed to recreate daemon with sess-beta: %v", err)
	}
	if d2.SessionID() != "sess-beta" {
		t.Errorf("expected new session sess-beta, got %s", d2.SessionID())
	}
	if d1.State() != runner.StateClosed {
		t.Errorf("expected old daemon d1 to be closed")
	}
}

func TestParseMeminfo_EdgeCases(t *testing.T) {
	// Valid meminfo
	validData := `MemTotal:       16000000 kB
MemFree:         4000000 kB
MemAvailable:    8000000 kB
Buffers:          500000 kB
`
	avail, err := parseMeminfo(strings.NewReader(validData))
	if err != nil {
		t.Fatalf("unexpected error parsing valid meminfo: %v", err)
	}
	if avail != 0.5 {
		t.Errorf("expected avail 0.5, got %f", avail)
	}

	// Empty meminfo
	avail, err = parseMeminfo(strings.NewReader(""))
	if err != nil || avail != 1.0 {
		t.Errorf("expected 1.0 on empty meminfo, got %f, %v", avail, err)
	}

	// Total only
	avail, err = parseMeminfo(strings.NewReader("MemTotal: 1000 kB\n"))
	if err != nil || avail != 0.0 {
		t.Errorf("expected 0.0 on zero avail with positive total, got %f, %v", avail, err)
	}

	// Invalid floats
	avail, err = parseMeminfo(strings.NewReader("MemTotal: abc kB\nMemAvailable: def kB\n"))
	if err != nil || avail != 1.0 {
		t.Errorf("expected 1.0 on invalid floats, got %f, %v", avail, err)
	}
}

func TestWorkerPool_StopIdempotency(t *testing.T) {
	cfg := config.NewTestConfig()
	pool := New(cfg, WorkerPoolConfig{})
	pool.Stop()
	// Second call should hit the p.stopped == true early return
	pool.Stop()
}

