package queue

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/runner"
)

func TestWorkerPool_UnifiedProcessPoolWiring(t *testing.T) {
	t.Run("NilWorkerPool_ReturnsNil", func(t *testing.T) {
		var p *WorkerPool
		if got := p.ProcessPool(); got != nil {
			t.Fatalf("expected nil ProcessPool from nil WorkerPool, got %v", got)
		}
	})

	t.Run("DefaultConfig_ProcessPoolIsNil", func(t *testing.T) {
		p := New(nil, WorkerPoolConfig{})
		if got := p.ProcessPool(); got != nil {
			t.Fatalf("expected nil ProcessPool by default, got %v", got)
		}
	})

	t.Run("ConfiguredProcessPool_PopulatesAndAccesses", func(t *testing.T) {
		mockSpawner := runner.NewMockDaemonSpawner()
		unifiedPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
			PrewarmedTargets: []string{"kiosk", "ephemeral:classifier", "ephemeral:summarizer"},
			DefaultModel:     "gemini-2.5-flash",
		}, mockSpawner)
		defer func() {
			if err := unifiedPool.Close(); err != nil {
				t.Logf("cleanup unified pool: %v", err)
			}
		}()

		p := New(nil, WorkerPoolConfig{
			ProcessPool: unifiedPool,
		})

		if got := p.ProcessPool(); got != unifiedPool {
			t.Fatalf("expected ProcessPool to return %p, got %p", unifiedPool, got)
		}
	})

	t.Run("MaintenanceLoopRunsWithoutMemoryPressureEviction", func(t *testing.T) {
		tempDir := t.TempDir()
		cfg := config.NewTestConfig(func(d *config.ConfigData) {
			d.MaxConcurrentDaemons = 5
			d.DaemonIdleTimeout = 1 * time.Hour
			d.DataDir = tempDir
		})

		mockSpawner := runner.NewMockDaemonSpawner()
		unifiedPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
			PrewarmedTargets: []string{"kiosk"},
			DefaultModel:     "gemini-2.5-flash",
		}, mockSpawner)
		defer func() {
			if err := unifiedPool.Close(); err != nil {
				t.Logf("cleanup unified pool: %v", err)
			}
		}()

		p := New(cfg, WorkerPoolConfig{
			ProcessPool:         unifiedPool,
			MaintenanceInterval: 10 * time.Millisecond,
		})

		var memCheckerCalled atomic.Int32
		if p.DaemonPool() != nil {
			p.DaemonPool().SetMemoryChecker(func() (float64, error) {
				memCheckerCalled.Add(1)
				return 0.05, nil
			})
		}
		if p.VoiceDaemonPool() != nil {
			p.VoiceDaemonPool().SetMemoryChecker(func() (float64, error) {
				memCheckerCalled.Add(1)
				return 0.05, nil
			})
		}

		p.Start()

		// Allow maintenance loop to tick multiple times
		time.Sleep(50 * time.Millisecond)

		// Explicit call to runMaintenance if accessible, or relying on ticker ticks
		p.Stop()

		if count := memCheckerCalled.Load(); count != 0 {
			t.Errorf("expected 0 memory checker calls in maintenance loop, got %d", count)
		}
	})
}
